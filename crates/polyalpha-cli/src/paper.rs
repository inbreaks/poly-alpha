use anyhow::{anyhow, Context, Result};
use chrono::{Local, TimeZone};
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast::error::TryRecvError, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{self, client::IntoClientRequest, http::HeaderValue, Message},
    MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;

use polyalpha_audit::{
    AuditAnomalyEvent, AuditCheckpoint, AuditCounterSnapshot, AuditEventKind, AuditEventPayload,
    AuditGateResult, AuditObservedMarket, AuditSessionManifest, AuditSessionStatus,
    AuditSessionSummary, AuditSeverity, AuditWarehouse, AuditWriter, EquityMarkEvent,
    EvaluationSkipEvent, ExecutionAuditEvent, GateDecisionEvent, MarketMarkEvent, NewAuditEvent,
    PositionMarkEvent, SignalEmittedEvent,
};
use polyalpha_core::{
    asset_key_from_cex_symbol, cex_venue_symbol, create_channels, AlphaEngine, AsyncClassification,
    CommandKind, CommandStatus, CommandView, ConnectionInfo, ConnectionStatus,
    EffectiveBasisConfig, EngineParams, EngineWarning, EvaluableStatus, EvaluationStats, EventKind,
    Exchange, ExecutionEvent, ExecutionPipelineView, ExecutionResult, Fill, HedgeState,
    InstrumentKind, MarketConfig, MarketDataEvent, MarketDataMode, MarketDataSource, MarketPhase,
    MarketRetentionReason, MarketTier, MarketView, MonitorAssetStrategyConfig, MonitorEvent,
    MonitorEventPayload, MonitorRuntimeStats, MonitorSocketServer, MonitorState,
    MonitorStrategyConfig, OpenCandidate, OrderSide, OrderStatus, PerformanceMetrics,
    PlanningDiagnostics, PlanningIntent, PolyShares, Position, Price, RecoveryDecisionReason,
    ResidualSnapshot, RiskManager, Settings, SignalStrength, StrategyHealthView,
    StrategyMarketScopeConfig, Symbol, SymbolRegistry, TokenSide, TradePlan, TradeView,
    UsdNotional, VenueQuantity, PLANNING_SCHEMA_VERSION,
};
use polyalpha_data::{
    BinanceFuturesDataSource, DataManager, OkxMarketDataSource, PolyBookLevel, PolyBookUpdate,
    PolymarketLiveDataSource,
};
use polyalpha_engine::{
    BasisStrategySnapshot, MarketOverrideConfig, SimpleAlphaEngine, SimpleEngineConfig,
};
#[cfg(test)]
use polyalpha_executor::dry_run::DryRunExecutor;
use polyalpha_executor::{
    dry_run::DryRunOrderSnapshot, ExecutionManager, InMemoryOrderbookProvider, PreviewIntentError,
};
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

use crate::args::{LiveExecutorMode, PaperInspectFormat};
use crate::commands::{open_plan_risk_rejection_reason, preview_open_plan_detailed, select_market};
use crate::market_pool::{
    active_open_cooldown_reason, active_open_cooldown_remaining_ms, clear_expired_open_cooldown,
    cooldown_gate_reason, maybe_start_open_cooldown, note_focus_activity, note_tradeable_activity,
    MarketPoolState,
};
#[cfg(test)]
use crate::runtime::open_intent_from_candidate as runtime_open_intent_from_candidate;
use crate::runtime::{
    apply_orderbook_snapshot, build_data_manager, build_execution_stack,
    close_intent_for_symbol as runtime_close_intent_for_symbol,
    delta_rebalance_intent_for_symbol as runtime_delta_rebalance_intent_for_symbol,
    force_exit_intent_for_symbol as runtime_force_exit_intent_for_symbol, RuntimeExecutionMode,
    RuntimeExecutor,
};
use crate::strategy_scope::{main_strategy_rejection_reason, strategy_rejection_label_zh};

const MAX_EVENT_LOGS: usize = 128;
const MAX_MONITOR_EVENTS: usize = 48;
const MAX_MONITOR_LOW_PRIORITY_EVENTS: usize = 8;
const MAX_MONITOR_PINNED_TRADE_CLOSED_EVENTS: usize = 4;
const MAX_RECENT_COMMANDS: usize = 24;
const MAX_RECENT_TRADES: usize = 48;
const MAX_EQUITY_POINTS: usize = 2_048;
const REPORT_WIDTH: usize = 78;
const SKIP_EVENT_DEBOUNCE_MS: u64 = 30_000;
const MONITOR_PUBLISH_THROTTLE_MS: u64 = 200;
const AUDIT_SUMMARY_WRITE_THROTTLE_MS: u64 = 1_000;
const MULTI_MARKET_BOOTSTRAP_MAX_CONCURRENCY: usize = 12;
const MULTI_MARKET_WARMUP_MAX_CONCURRENCY: usize = 8;
const MULTI_MARKET_FUNDING_MAX_CONCURRENCY: usize = 8;
const MULTI_MARKET_SYNC_WARMUP_THRESHOLD: usize = 16;
const WS_CONNECT_TIMEOUT_MS: u64 = 10_000;
const WS_CONNECTED_STATUS_REFRESH_MS: u64 = 1_000;
const MARKET_TRADEABLE_GRACE_MULTIPLIER: u64 = 3;
const MARKET_FOCUS_GRACE_MULTIPLIER: u64 = 12;
const MARKET_NEAR_OPPORTUNITY_FRACTION: f64 = 0.75;
const MAX_WARMUP_FETCH_KLINES: u16 = 1_500;
const MIN_WARMUP_FETCH_HEADROOM: u16 = 60;
const STRATEGY_READINESS_ALERT_GRACE_MS: u64 = 45_000;
const STRATEGY_READINESS_ALERT_INTERVAL_MS: u64 = 60_000;

type EventDetails = HashMap<String, String>;
const PAPER_ARTIFACT_SCHEMA_VERSION: u32 = PLANNING_SCHEMA_VERSION;

fn default_artifact_trading_mode() -> polyalpha_core::TradingMode {
    polyalpha_core::TradingMode::Paper
}

fn deserialize_paper_schema_version<'de, D>(deserializer: D) -> std::result::Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let schema_version = u32::deserialize(deserializer)?;
    if schema_version == PAPER_ARTIFACT_SCHEMA_VERSION {
        Ok(schema_version)
    } else {
        Err(D::Error::custom(format!(
            "incompatible paper artifact schema_version: expected {}, got {}",
            PAPER_ARTIFACT_SCHEMA_VERSION, schema_version
        )))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EvaluationSkipReason {
    NoFreshPolySample,
    PolyPriceStale,
    CexPriceStale,
    DataMisaligned,
    ConnectionLost,
    NoCexReference,
}

impl EvaluationSkipReason {
    fn key(self) -> &'static str {
        match self {
            Self::NoFreshPolySample => "no_fresh_poly_sample",
            Self::PolyPriceStale => "poly_price_stale",
            Self::CexPriceStale => "cex_price_stale",
            Self::DataMisaligned => "data_misaligned",
            Self::ConnectionLost => "connection_lost",
            Self::NoCexReference => "no_cex_reference",
        }
    }

    fn label_zh(self) -> &'static str {
        match self {
            Self::NoFreshPolySample => "暂无新的 Polymarket 行情样本",
            Self::PolyPriceStale => "Polymarket 行情已超时",
            Self::CexPriceStale => "交易所行情已超时",
            Self::DataMisaligned => "Polymarket 与交易所时间未对齐",
            Self::ConnectionLost => "行情连接异常",
            Self::NoCexReference => "缺少可用的交易所参考价",
        }
    }
}

#[derive(Clone, Debug)]
enum RuntimeTrigger {
    Candidate(OpenCandidate),
    Intent {
        intent: PlanningIntent,
        timestamp_ms: u64,
    },
}

impl RuntimeTrigger {
    fn id(&self) -> &str {
        match self {
            Self::Candidate(candidate) => &candidate.candidate_id,
            Self::Intent { intent, .. } => intent.intent_id(),
        }
    }

    fn correlation_id(&self) -> &str {
        match self {
            Self::Candidate(candidate) => &candidate.correlation_id,
            Self::Intent { intent, .. } => intent.correlation_id(),
        }
    }

    fn symbol(&self) -> &Symbol {
        match self {
            Self::Candidate(candidate) => &candidate.symbol,
            Self::Intent { intent, .. } => intent.symbol(),
        }
    }

    fn timestamp_ms(&self) -> u64 {
        match self {
            Self::Candidate(candidate) => candidate.timestamp_ms,
            Self::Intent { timestamp_ms, .. } => *timestamp_ms,
        }
    }

    fn candidate(&self) -> Option<&OpenCandidate> {
        match self {
            Self::Candidate(candidate) => Some(candidate),
            Self::Intent { .. } => None,
        }
    }

    fn intent(&self) -> Option<&PlanningIntent> {
        match self {
            Self::Candidate(_) => None,
            Self::Intent { intent, .. } => Some(intent),
        }
    }

    fn is_close_like(&self) -> bool {
        matches!(
            self.intent(),
            Some(
                PlanningIntent::ClosePosition { .. }
                    | PlanningIntent::DeltaRebalance { .. }
                    | PlanningIntent::ResidualRecovery { .. }
                    | PlanningIntent::ForceExit { .. }
            )
        )
    }
}

#[derive(Clone)]
struct ExchangeConnectionTracker {
    exchange: Exchange,
    manager: DataManager,
    status: Arc<Mutex<ConnectionStatus>>,
    active_streams: Arc<Mutex<HashSet<String>>>,
    last_connected_publish_ms: Arc<Mutex<Option<u64>>>,
}

impl ExchangeConnectionTracker {
    fn new(exchange: Exchange, manager: DataManager) -> Self {
        Self {
            exchange,
            manager,
            status: Arc::new(Mutex::new(ConnectionStatus::Disconnected)),
            active_streams: Arc::new(Mutex::new(HashSet::new())),
            last_connected_publish_ms: Arc::new(Mutex::new(None)),
        }
    }

    fn mark_connecting(&self) {
        if self
            .active_streams
            .lock()
            .map(|guard| !guard.is_empty())
            .unwrap_or(false)
        {
            return;
        }
        self.set_status(ConnectionStatus::Connecting);
    }

    fn mark_connected(&self, stream_key: &str) {
        if let Ok(mut guard) = self.active_streams.lock() {
            guard.insert(stream_key.to_owned());
        }
        self.set_status(ConnectionStatus::Connected);
    }

    fn mark_disconnected(&self, stream_key: &str) {
        let active_count = if let Ok(mut guard) = self.active_streams.lock() {
            guard.remove(stream_key);
            guard.len()
        } else {
            0
        };

        if active_count == 0 {
            self.set_status(ConnectionStatus::Reconnecting);
        }
    }

    fn reaffirm_connected(&self) {
        self.reaffirm_connected_at(now_millis());
    }

    fn reaffirm_connected_at(&self, now_ms: u64) {
        if !self
            .active_streams
            .lock()
            .map(|guard| !guard.is_empty())
            .unwrap_or(false)
        {
            return;
        }

        if self
            .status
            .lock()
            .map(|guard| *guard != ConnectionStatus::Connected)
            .unwrap_or(true)
        {
            return;
        }

        let should_publish = match self.last_connected_publish_ms.lock() {
            Ok(mut guard) => {
                if guard.is_some_and(|last_ms| {
                    now_ms.saturating_sub(last_ms) < WS_CONNECTED_STATUS_REFRESH_MS
                }) {
                    false
                } else {
                    *guard = Some(now_ms);
                    true
                }
            }
            Err(_) => false,
        };

        if should_publish {
            let _ = self.manager.publish(MarketDataEvent::ConnectionEvent {
                exchange: self.exchange,
                status: ConnectionStatus::Connected,
            });
        }
    }

    fn set_status(&self, new_status: ConnectionStatus) {
        let mut guard = match self.status.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if *guard == new_status {
            return;
        }
        *guard = new_status;
        drop(guard);
        if let Ok(mut publish_guard) = self.last_connected_publish_ms.lock() {
            *publish_guard = (new_status == ConnectionStatus::Connected).then_some(now_millis());
        }
        eprintln!("ws status {:?} -> {:?}", self.exchange, new_status);
        tracing::info!("ws status {:?} -> {:?}", self.exchange, new_status);
        let _ = self.manager.publish(MarketDataEvent::ConnectionEvent {
            exchange: self.exchange,
            status: new_status,
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinanceWsTransportMode {
    PerSymbol,
    Combined,
}

fn select_binance_ws_transport_mode(settings: &Settings) -> BinanceWsTransportMode {
    if settings.strategy.market_data.binance_combined_ws_enabled {
        BinanceWsTransportMode::Combined
    } else {
        BinanceWsTransportMode::PerSymbol
    }
}

fn binance_combined_ws_requested_extensions() -> Option<&'static str> {
    // tungstenite 0.24 rejects non-zero RSV bits and does not implement
    // permessage-deflate decoding, so requesting compression causes the
    // combined socket to fail immediately after the first compressed frame.
    None
}

#[derive(Clone, Debug, Default)]
struct LocalPolyBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_trade_price: Option<Price>,
    sequence: u64,
}

impl LocalPolyBook {
    fn from_update_with_previous(update: &PolyBookUpdate, previous_sequence: u64) -> Self {
        let mut book = Self::default();
        for level in &update.bids {
            book.bids.insert(level.price.0, level.shares.0);
        }
        for level in &update.asks {
            book.asks.insert(level.price.0, level.shares.0);
        }
        book.last_trade_price = update.last_trade_price;
        book.sequence = normalized_ws_poly_sequence(previous_sequence, update.sequence);
        book
    }

    fn apply_level(&mut self, side: PolymarketBookSide, price: Decimal, size: Decimal) {
        let levels = match side {
            PolymarketBookSide::Bid => &mut self.bids,
            PolymarketBookSide::Ask => &mut self.asks,
        };
        if size <= Decimal::ZERO {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
    }

    fn next_sequence(&self) -> u64 {
        normalized_ws_poly_sequence(self.sequence, 0)
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
            received_at_ms: now_millis(),
            sequence,
            last_trade_price: self.last_trade_price,
        }
    }
}

fn normalized_ws_poly_sequence(previous_sequence: u64, incoming_sequence: u64) -> u64 {
    incoming_sequence
        .max(previous_sequence.saturating_add(1))
        .max(1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolymarketBookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug)]
struct PolymarketPriceChange {
    asset_id: String,
    price: Decimal,
    size: Decimal,
    side: PolymarketBookSide,
    timestamp_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PolymarketWsDiagnosticsSnapshot {
    text_frames: u64,
    price_change_messages: u64,
    book_messages: u64,
    orderbook_updates: u64,
}

#[derive(Clone, Default)]
struct PolymarketWsDiagnostics {
    inner: Arc<PolymarketWsDiagnosticsInner>,
}

#[derive(Default)]
struct PolymarketWsDiagnosticsInner {
    text_frames: AtomicU64,
    price_change_messages: AtomicU64,
    book_messages: AtomicU64,
    orderbook_updates: AtomicU64,
}

impl PolymarketWsDiagnostics {
    fn record_text_frame(&self) {
        self.inner.text_frames.fetch_add(1, Ordering::Relaxed);
    }

    fn record_price_change_message(&self) {
        self.inner
            .price_change_messages
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_book_message(&self) {
        self.inner.book_messages.fetch_add(1, Ordering::Relaxed);
    }

    fn add_orderbook_updates(&self, count: u64) {
        self.inner
            .orderbook_updates
            .fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PolymarketWsDiagnosticsSnapshot {
        PolymarketWsDiagnosticsSnapshot {
            text_frames: self.inner.text_frames.load(Ordering::Relaxed),
            price_change_messages: self.inner.price_change_messages.load(Ordering::Relaxed),
            book_messages: self.inner.book_messages.load(Ordering::Relaxed),
            orderbook_updates: self.inner.orderbook_updates.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Default)]
struct PaperStats {
    ticks_processed: usize,
    market_events: usize,
    signals_seen: usize,
    signals_rejected: usize,
    rejection_reasons: HashMap<String, u64>,
    evaluation_attempts: u64,
    evaluable_passes: u64,
    evaluation_skipped: u64,
    execution_rejected: u64,
    skip_reasons: HashMap<String, u64>,
    evaluable_status_counts: HashMap<String, u64>,
    async_classification_counts: HashMap<String, u64>,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
    poll_errors: usize,
    snapshot_resync_count: usize,
    funding_refresh_count: usize,
    trades_closed: usize,
    market_data_rx_lag_events: u64,
    market_data_rx_lagged_messages: u64,
    market_data_tick_drain_last: u64,
    market_data_tick_drain_max: u64,
    total_pnl_usd: f64,
    last_skip_event_ms: HashMap<String, u64>,
    last_delta_rebalance_at_ms: HashMap<Symbol, u64>,
    polymarket_ws_diagnostics: PolymarketWsDiagnostics,
}

fn record_market_data_rx_lag(stats: &mut PaperStats, lagged_messages: u64) {
    stats.market_data_rx_lag_events = stats.market_data_rx_lag_events.saturating_add(1);
    stats.market_data_rx_lagged_messages = stats
        .market_data_rx_lagged_messages
        .saturating_add(lagged_messages);
}

fn record_market_data_tick_drain(stats: &mut PaperStats, processed_since_last_tick: &mut u64) {
    let drained = *processed_since_last_tick;
    stats.market_data_tick_drain_last = drained;
    stats.market_data_tick_drain_max = stats.market_data_tick_drain_max.max(drained);
    *processed_since_last_tick = 0;
}

#[derive(Clone, Debug, Default)]
struct MarketWarmupStatus {
    cex_klines: usize,
    yes_history_len: usize,
    no_history_len: usize,
    cex_fetch_error: Option<String>,
    poly_history_errors: Vec<(TokenSide, String)>,
}

#[derive(Clone, Debug)]
struct StrategyReadinessAlert {
    kind: &'static str,
    summary: String,
    details: Option<EventDetails>,
}

#[derive(Clone, Debug, Default)]
struct ObservedState {
    poly_yes_mid: Option<Decimal>,
    poly_no_mid: Option<Decimal>,
    cex_mid: Option<Decimal>,
    poly_yes_updated_at_ms: Option<u64>,
    poly_no_updated_at_ms: Option<u64>,
    cex_updated_at_ms: Option<u64>,
    poly_yes_version: Option<ObservedDataVersion>,
    poly_no_version: Option<ObservedDataVersion>,
    cex_version: Option<ObservedDataVersion>,
    market_phase: Option<MarketPhase>,
    connections: HashMap<String, ConnectionStatus>,
    connection_updated_at_ms: HashMap<String, u64>,
    connection_reconnect_count: HashMap<String, u64>,
    connection_disconnect_count: HashMap<String, u64>,
    connection_last_disconnect_at_ms: HashMap<String, u64>,
    market_pool: MarketPoolState,
}

impl ObservedState {
    fn latest_poly_update_ms(&self) -> Option<u64> {
        match (self.poly_yes_updated_at_ms, self.poly_no_updated_at_ms) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct ObservedDataVersion {
    exchange_timestamp_ms: u64,
    sequence: u64,
    received_at_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct FreshnessThresholds {
    poly_open_max_quote_age_ms: u64,
    cex_open_max_quote_age_ms: u64,
    close_max_quote_age_ms: u64,
    max_cross_leg_skew_ms: u64,
    borderline_poly_quote_age_ms: u64,
}

#[derive(Clone, Debug)]
struct AsyncMarketSnapshot {
    poly_updated_at_ms: Option<u64>,
    cex_updated_at_ms: Option<u64>,
    poly_quote_age_ms: Option<u64>,
    cex_quote_age_ms: Option<u64>,
    cross_leg_skew_ms: Option<u64>,
    transport_idle_ms_by_connection: HashMap<String, u64>,
    evaluable_status: EvaluableStatus,
    async_classification: AsyncClassification,
}

fn global_basis_config(settings: &Settings) -> EffectiveBasisConfig {
    settings.strategy.basis.effective_for_asset_key("")
}

fn effective_basis_config_for_market(
    settings: &Settings,
    market: &MarketConfig,
) -> EffectiveBasisConfig {
    settings
        .strategy
        .basis
        .effective_for_cex_symbol(&market.cex_symbol)
}

fn effective_basis_config_for_symbol(settings: &Settings, symbol: &Symbol) -> EffectiveBasisConfig {
    settings
        .markets
        .iter()
        .find(|market| market.symbol == *symbol)
        .map(|market| effective_basis_config_for_market(settings, market))
        .unwrap_or_else(|| global_basis_config(settings))
}

fn freshness_thresholds(settings: &Settings, basis: &EffectiveBasisConfig) -> FreshnessThresholds {
    let market_data = &settings.strategy.market_data;
    match settings.strategy.market_data.mode {
        MarketDataMode::Ws => FreshnessThresholds {
            poly_open_max_quote_age_ms: market_data.resolved_poly_open_max_quote_age_ms(),
            cex_open_max_quote_age_ms: market_data.resolved_cex_open_max_quote_age_ms(),
            close_max_quote_age_ms: market_data.resolved_close_max_quote_age_ms(),
            max_cross_leg_skew_ms: market_data.resolved_max_cross_leg_skew_ms(),
            borderline_poly_quote_age_ms: market_data.resolved_borderline_poly_quote_age_ms(),
        },
        MarketDataMode::Poll => {
            let age_limit_ms = basis.max_data_age_minutes.saturating_mul(60_000);
            let skew_limit_ms = basis.max_time_diff_minutes.saturating_mul(60_000);
            FreshnessThresholds {
                poly_open_max_quote_age_ms: age_limit_ms,
                cex_open_max_quote_age_ms: age_limit_ms,
                close_max_quote_age_ms: age_limit_ms,
                max_cross_leg_skew_ms: skew_limit_ms,
                borderline_poly_quote_age_ms: age_limit_ms.saturating_mul(8) / 10,
            }
        }
    }
}

fn preferred_poly_update_ms(
    observed: &ObservedState,
    token_side: Option<TokenSide>,
) -> Option<u64> {
    match token_side {
        Some(TokenSide::Yes) => observed.poly_yes_updated_at_ms,
        Some(TokenSide::No) => observed.poly_no_updated_at_ms,
        None => observed.latest_poly_update_ms(),
    }
}

fn quote_age_ms(now_ms: u64, updated_at_ms: Option<u64>) -> Option<u64> {
    updated_at_ms.map(|updated_at_ms| now_ms.saturating_sub(updated_at_ms))
}

fn absolute_time_diff_ms(left_ms: Option<u64>, right_ms: Option<u64>) -> Option<u64> {
    match (left_ms, right_ms) {
        (Some(left_ms), Some(right_ms)) if left_ms >= right_ms => Some(left_ms - right_ms),
        (Some(left_ms), Some(right_ms)) => Some(right_ms - left_ms),
        _ => None,
    }
}

fn connection_name(exchange: Exchange) -> String {
    format!("{exchange:?}")
}

fn tracked_connection_status(
    observed: &ObservedState,
    exchange: Exchange,
) -> Option<ConnectionStatus> {
    observed
        .connections
        .get(&connection_name(exchange))
        .copied()
}

fn connection_is_impaired(status: Option<ConnectionStatus>) -> bool {
    matches!(
        status,
        None | Some(ConnectionStatus::Connecting)
            | Some(ConnectionStatus::Reconnecting)
            | Some(ConnectionStatus::Disconnected)
    )
}

fn classify_async_market(
    now_ms: u64,
    market: &MarketConfig,
    observed: &ObservedState,
    thresholds: FreshnessThresholds,
    token_side: Option<TokenSide>,
    use_close_thresholds: bool,
    require_connections: bool,
) -> AsyncMarketSnapshot {
    let poly_updated_at_ms = preferred_poly_update_ms(observed, token_side);
    let cex_updated_at_ms = observed.cex_updated_at_ms;
    let poly_quote_age_ms = quote_age_ms(now_ms, poly_updated_at_ms);
    let cex_quote_age_ms = quote_age_ms(now_ms, cex_updated_at_ms);
    let cross_leg_skew_ms = absolute_time_diff_ms(poly_updated_at_ms, cex_updated_at_ms);
    let transport_idle_ms_by_connection = observed
        .connection_updated_at_ms
        .iter()
        .map(|(name, updated_at_ms)| (name.clone(), now_ms.saturating_sub(*updated_at_ms)))
        .collect::<HashMap<_, _>>();
    let poly_limit = if use_close_thresholds {
        thresholds.close_max_quote_age_ms
    } else {
        thresholds.poly_open_max_quote_age_ms
    };
    let cex_limit = if use_close_thresholds {
        thresholds.close_max_quote_age_ms
    } else {
        thresholds.cex_open_max_quote_age_ms
    };
    let poly_connection = tracked_connection_status(observed, Exchange::Polymarket);
    let cex_connection = tracked_connection_status(observed, market.hedge_exchange);

    let (evaluable_status, async_classification) = if require_connections
        && (connection_is_impaired(poly_connection) || connection_is_impaired(cex_connection))
    {
        (
            EvaluableStatus::ConnectionImpaired,
            AsyncClassification::ConnectionImpaired,
        )
    } else if poly_updated_at_ms.is_none() {
        (
            EvaluableStatus::NotEvaluableNoPoly,
            AsyncClassification::NoPolySample,
        )
    } else if cex_updated_at_ms.is_none() {
        (
            EvaluableStatus::NotEvaluableNoCex,
            AsyncClassification::NoCexReference,
        )
    } else if poly_quote_age_ms.is_some_and(|age_ms| age_ms > poly_limit) {
        (
            EvaluableStatus::PolyQuoteStale,
            AsyncClassification::PolyQuoteStale,
        )
    } else if cex_quote_age_ms.is_some_and(|age_ms| age_ms > cex_limit) {
        (
            EvaluableStatus::CexQuoteStale,
            AsyncClassification::CexQuoteStale,
        )
    } else if cross_leg_skew_ms.is_some_and(|skew_ms| skew_ms > thresholds.max_cross_leg_skew_ms) {
        (
            EvaluableStatus::NotEvaluableMisaligned,
            AsyncClassification::Misaligned,
        )
    } else {
        let poly_age_ms = poly_quote_age_ms.unwrap_or_default();
        let cex_age_ms = cex_quote_age_ms.unwrap_or_default();
        let skew_ms = cross_leg_skew_ms.unwrap_or_default();
        let borderline_skew_ms = thresholds.max_cross_leg_skew_ms.saturating_mul(8) / 10;

        let classification = if poly_age_ms > cex_age_ms
            && (poly_age_ms >= thresholds.borderline_poly_quote_age_ms
                || skew_ms >= borderline_skew_ms)
        {
            AsyncClassification::PolyLagBorderline
        } else if poly_age_ms > cex_age_ms {
            AsyncClassification::PolyLagAcceptable
        } else {
            AsyncClassification::BalancedFresh
        };

        (EvaluableStatus::Evaluable, classification)
    };

    AsyncMarketSnapshot {
        poly_updated_at_ms,
        cex_updated_at_ms,
        poly_quote_age_ms,
        cex_quote_age_ms,
        cross_leg_skew_ms,
        transport_idle_ms_by_connection,
        evaluable_status,
        async_classification,
    }
}

fn build_audit_observed_market(
    now_ms: u64,
    market: &MarketConfig,
    observed: &ObservedState,
    settings: &Settings,
    token_side: Option<TokenSide>,
    use_close_thresholds: bool,
    basis: &EffectiveBasisConfig,
) -> AuditObservedMarket {
    let async_snapshot = classify_async_market(
        now_ms,
        market,
        observed,
        freshness_thresholds(settings, basis),
        token_side,
        use_close_thresholds,
        require_connected_inputs(basis),
    );

    AuditObservedMarket {
        symbol: market.symbol.0.clone(),
        poly_yes_mid: observed.poly_yes_mid.and_then(|value| value.to_f64()),
        poly_no_mid: observed.poly_no_mid.and_then(|value| value.to_f64()),
        cex_mid: observed.cex_mid.and_then(|value| value.to_f64()),
        poly_updated_at_ms: async_snapshot.poly_updated_at_ms,
        cex_updated_at_ms: async_snapshot.cex_updated_at_ms,
        poly_quote_age_ms: async_snapshot.poly_quote_age_ms,
        cex_quote_age_ms: async_snapshot.cex_quote_age_ms,
        cross_leg_skew_ms: async_snapshot.cross_leg_skew_ms,
        market_phase: observed
            .market_phase
            .as_ref()
            .map(|phase| format!("{phase:?}")),
        connections: observed.connections.clone(),
        transport_idle_ms_by_connection: async_snapshot.transport_idle_ms_by_connection,
        evaluable_status: async_snapshot.evaluable_status,
        async_classification: async_snapshot.async_classification,
    }
}

fn require_connected_inputs(basis: &EffectiveBasisConfig) -> bool {
    basis.enable_freshness_check && basis.reject_on_disconnect
}

fn market_retention_reason(
    has_position: bool,
    active_plan_priority: Option<&str>,
) -> MarketRetentionReason {
    match active_plan_priority {
        Some("force_exit") => MarketRetentionReason::ForceExit,
        Some("residual_recovery") => MarketRetentionReason::ResidualRecovery,
        Some("close_position") => MarketRetentionReason::CloseInProgress,
        Some("delta_rebalance") => MarketRetentionReason::DeltaRebalance,
        _ if has_position => MarketRetentionReason::HasPosition,
        _ => MarketRetentionReason::None,
    }
}

fn note_focus_market_activity(observed: &mut ObservedState, now_ms: u64) {
    note_focus_activity(&mut observed.market_pool, now_ms);
}

fn note_tradeable_market_activity(observed: &mut ObservedState, now_ms: u64) {
    note_tradeable_activity(&mut observed.market_pool, now_ms);
}

fn market_recent_within(last_seen_at_ms: Option<u64>, now_ms: u64, grace_ms: u64) -> bool {
    last_seen_at_ms.is_some_and(|seen_at_ms| now_ms.saturating_sub(seen_at_ms) <= grace_ms)
}

fn market_tradeable_grace_ms(settings: &Settings) -> u64 {
    settings
        .strategy
        .market_data
        .max_stale_ms
        .max(5_000)
        .saturating_mul(MARKET_TRADEABLE_GRACE_MULTIPLIER)
}

fn market_focus_grace_ms(settings: &Settings) -> u64 {
    settings
        .strategy
        .market_data
        .max_stale_ms
        .max(5_000)
        .saturating_mul(MARKET_FOCUS_GRACE_MULTIPLIER)
}

fn async_market_is_tradeable(
    evaluable_status: EvaluableStatus,
    async_classification: AsyncClassification,
) -> bool {
    evaluable_status == EvaluableStatus::Evaluable
        && matches!(
            async_classification,
            AsyncClassification::BalancedFresh | AsyncClassification::PolyLagAcceptable
        )
}

fn async_snapshot_is_tradeable(snapshot: &AsyncMarketSnapshot) -> bool {
    async_market_is_tradeable(snapshot.evaluable_status, snapshot.async_classification)
}

fn audit_observed_is_tradeable(observed: &AuditObservedMarket) -> bool {
    async_market_is_tradeable(observed.evaluable_status, observed.async_classification)
}

fn clear_market_pool_cooldown_if_needed(observed: &mut ObservedState, now_ms: u64) {
    clear_expired_open_cooldown(&mut observed.market_pool, now_ms);
}

fn market_pool_cooldown_reason(observed: &ObservedState, now_ms: u64) -> Option<&str> {
    active_open_cooldown_reason(&observed.market_pool, now_ms)
}

fn market_pool_cooldown_remaining_ms(observed: &ObservedState, now_ms: u64) -> Option<u64> {
    active_open_cooldown_remaining_ms(&observed.market_pool, now_ms)
}

fn maybe_start_market_pool_cooldown(
    observed: &mut ObservedState,
    settings: &Settings,
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

fn maybe_start_market_pool_cooldown_from_message(
    observed: &mut ObservedState,
    settings: &Settings,
    now_ms: u64,
    message: &str,
) -> Option<String> {
    let reason = extract_plan_rejection_reason(message).unwrap_or_else(|| message.to_owned());
    maybe_start_market_pool_cooldown(observed, settings, now_ms, &reason).then_some(reason)
}

fn maybe_start_market_pool_cooldown_from_rejections(
    observed: &mut ObservedState,
    settings: &Settings,
    now_ms: u64,
    rejection_reasons: &[String],
) -> Option<String> {
    rejection_reasons.iter().find_map(|reason| {
        maybe_start_market_pool_cooldown_from_message(observed, settings, now_ms, reason)
    })
}

fn market_pool_gate_reason(
    observed: &ObservedState,
    retention_reason: MarketRetentionReason,
    tradeable_now: bool,
    now_ms: u64,
) -> String {
    if let Some(reason) = market_pool_cooldown_reason(observed, now_ms) {
        return cooldown_gate_reason(reason);
    }
    if retention_reason != MarketRetentionReason::None {
        return format!("retained_{}", retention_reason.as_str());
    }
    if !tradeable_now {
        return "not_tradeable_pool".to_owned();
    }
    "not_tradeable_pool".to_owned()
}

fn market_is_near_opportunity(
    settings: &Settings,
    snapshot: Option<&polyalpha_engine::BasisStrategySnapshot>,
) -> bool {
    let Some(snapshot) = snapshot else {
        return false;
    };

    let entry_z = settings
        .strategy
        .basis
        .entry_z_score_threshold
        .to_f64()
        .unwrap_or_default()
        .abs();
    let min_basis_bps = settings
        .strategy
        .basis
        .min_basis_bps
        .to_f64()
        .unwrap_or_default()
        .abs();

    if entry_z > 0.0
        && snapshot
            .z_score
            .is_some_and(|z_score| z_score.abs() >= entry_z * MARKET_NEAR_OPPORTUNITY_FRACTION)
    {
        return true;
    }

    (snapshot.signal_basis.abs() * 10_000.0) >= min_basis_bps * MARKET_NEAR_OPPORTUNITY_FRACTION
}

fn determine_market_tier(
    settings: &Settings,
    tradeable_now: bool,
    retention_reason: MarketRetentionReason,
    near_opportunity: bool,
    last_focus_at_ms: Option<u64>,
    last_tradeable_at_ms: Option<u64>,
    open_cooldown_active: bool,
    now_ms: u64,
) -> MarketTier {
    if retention_reason != MarketRetentionReason::None {
        return MarketTier::Focus;
    }

    let focus_recent =
        market_recent_within(last_focus_at_ms, now_ms, market_focus_grace_ms(settings));
    let tradeable_recent = market_recent_within(
        last_tradeable_at_ms,
        now_ms,
        market_tradeable_grace_ms(settings),
    );

    if tradeable_now
        && (near_opportunity || focus_recent || tradeable_recent)
        && !open_cooldown_active
    {
        return MarketTier::Tradeable;
    }

    if near_opportunity || focus_recent || open_cooldown_active {
        return MarketTier::Focus;
    }

    MarketTier::Observation
}

fn audit_snapshot_for_candidate(
    settings: &Settings,
    registry: &SymbolRegistry,
    candidate: &OpenCandidate,
    observed: &ObservedState,
) -> Option<AuditObservedMarket> {
    registry.get_config(&candidate.symbol).map(|market| {
        let basis = effective_basis_config_for_market(settings, market);
        build_audit_observed_market(
            now_millis(),
            market,
            observed,
            settings,
            Some(candidate.token_side),
            false,
            &basis,
        )
    })
}

enum AuditCommand {
    Event(NewAuditEvent),
    RuntimeState {
        stats: PaperStats,
        monitor_state: MonitorState,
        tick_index: usize,
        now_ms: u64,
        record_marks: bool,
        write_checkpoint: bool,
        write_summary: bool,
        sync_warehouse: bool,
    },
    Finalize {
        stats: PaperStats,
        monitor_state: MonitorState,
        tick_index: usize,
        now_ms: u64,
        reply: std_mpsc::SyncSender<std::result::Result<(), String>>,
    },
    #[cfg(test)]
    Barrier {
        reply: std_mpsc::SyncSender<std::result::Result<(), String>>,
    },
    Shutdown {
        reply: Option<std_mpsc::SyncSender<()>>,
    },
}

struct PaperAuditWorker {
    data_dir: String,
    writer: AuditWriter,
    warehouse: AuditWarehouse,
    summary: AuditSessionSummary,
    warehouse_sync_degraded: bool,
    last_mark_tick_index: Option<u64>,
}

impl PaperAuditWorker {
    fn apply_runtime_state(
        &mut self,
        stats: &PaperStats,
        monitor_state: &MonitorState,
        tick_index: usize,
        now_ms: u64,
        record_marks: bool,
        write_checkpoint: bool,
        write_summary: bool,
        sync_warehouse: bool,
    ) -> Result<()> {
        self.refresh_summary(stats, monitor_state, tick_index, now_ms);

        if record_marks {
            self.record_marks(monitor_state, tick_index as u64, now_ms)?;
        }

        if write_checkpoint {
            self.summary.latest_checkpoint_ms = Some(now_ms);
            let checkpoint = build_audit_checkpoint(
                &self.summary.session_id,
                stats,
                monitor_state,
                tick_index,
                now_ms,
            );
            self.writer.write_checkpoint(&checkpoint)?;
            self.writer.write_summary(&self.summary)?;
        } else if write_summary {
            self.writer.write_summary(&self.summary)?;
        }

        if sync_warehouse {
            self.sync_warehouse_best_effort(now_ms)?;
        }

        Ok(())
    }

    fn finalize(
        &mut self,
        stats: &PaperStats,
        monitor_state: &MonitorState,
        tick_index: usize,
        now_ms: u64,
    ) -> Result<()> {
        self.record_marks(monitor_state, tick_index as u64, now_ms)?;
        self.refresh_summary(stats, monitor_state, tick_index, now_ms);
        self.summary.status = AuditSessionStatus::Completed;
        self.summary.ended_at_ms = Some(now_ms);
        self.summary.updated_at_ms = now_ms;
        self.summary.latest_checkpoint_ms = Some(now_ms);
        let checkpoint = build_audit_checkpoint(
            &self.summary.session_id,
            stats,
            monitor_state,
            tick_index,
            now_ms,
        );
        self.writer.write_checkpoint(&checkpoint)?;
        self.writer.write_summary(&self.summary)?;
        self.sync_warehouse_best_effort(now_ms)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush_raw()
    }

    fn record_marks(
        &mut self,
        monitor_state: &MonitorState,
        tick_index: u64,
        now_ms: u64,
    ) -> Result<()> {
        if self.last_mark_tick_index == Some(tick_index) {
            return Ok(());
        }
        self.record_event(NewAuditEvent {
            timestamp_ms: now_ms,
            kind: AuditEventKind::EquityMark,
            symbol: None,
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: None,
            summary: format!(
                "净值快照 tick={tick_index} equity={:.4}",
                monitor_state.performance.equity
            ),
            payload: AuditEventPayload::EquityMark(EquityMarkEvent {
                tick_index,
                performance: monitor_state.performance.clone(),
            }),
        })?;

        for market in &monitor_state.markets {
            self.record_event(NewAuditEvent {
                timestamp_ms: now_ms,
                kind: AuditEventKind::MarketMark,
                symbol: Some(market.symbol.clone()),
                signal_id: None,
                correlation_id: None,
                gate: None,
                result: None,
                reason: None,
                summary: format!("市场快照 {} tick={tick_index}", market.symbol),
                payload: AuditEventPayload::MarketMark(MarketMarkEvent {
                    tick_index,
                    market: market.clone(),
                }),
            })?;
        }

        for position in &monitor_state.positions {
            self.record_event(NewAuditEvent {
                timestamp_ms: now_ms,
                kind: AuditEventKind::PositionMark,
                symbol: Some(position.market.clone()),
                signal_id: None,
                correlation_id: None,
                gate: None,
                result: None,
                reason: None,
                summary: format!("持仓快照 {} tick={tick_index}", position.market),
                payload: AuditEventPayload::PositionMark(PositionMarkEvent {
                    tick_index,
                    position: position.clone(),
                }),
            })?;
        }

        self.last_mark_tick_index = Some(tick_index);
        Ok(())
    }

    fn refresh_summary(
        &mut self,
        stats: &PaperStats,
        monitor_state: &MonitorState,
        tick_index: usize,
        now_ms: u64,
    ) {
        self.summary.updated_at_ms = now_ms;
        self.summary.latest_tick_index = tick_index as u64;
        self.summary.counters = audit_counter_snapshot(stats);
        self.summary.latest_equity = Some(monitor_state.performance.equity);
        self.summary.latest_total_pnl_usd = Some(monitor_state.performance.total_pnl_usd);
        self.summary.latest_today_pnl_usd = Some(monitor_state.performance.today_pnl_usd);
        self.summary.latest_total_exposure_usd = Some(
            monitor_state
                .positions
                .iter()
                .map(position_exposure_notional)
                .sum::<f64>(),
        );
        self.summary.rejection_reasons = stats.rejection_reasons.clone();
        self.summary.skip_reasons = stats.skip_reasons.clone();
        self.summary.evaluable_status_counts = stats.evaluable_status_counts.clone();
        self.summary.async_classification_counts = stats.async_classification_counts.clone();
    }

    fn sync_warehouse(&self) -> Result<()> {
        self.warehouse
            .sync_session(&self.data_dir, &self.summary.session_id)
            .with_context(|| format!("同步审计会话 `{}` 到仓库失败", self.summary.session_id))
    }

    fn sync_warehouse_best_effort(&mut self, now_ms: u64) -> Result<()> {
        match self.sync_warehouse() {
            Ok(()) => {
                if self.warehouse_sync_degraded {
                    self.record_warehouse_sync_anomaly(
                        now_ms,
                        AuditSeverity::Info,
                        "warehouse_sync_recovered",
                        "审计仓库同步已恢复".to_owned(),
                    )?;
                    eprintln!("审计仓库同步已恢复：{}", self.summary.session_id);
                    self.warehouse_sync_degraded = false;
                }
                Ok(())
            }
            Err(err) => {
                let message = warehouse_sync_warning_message(&err);
                if !self.warehouse_sync_degraded {
                    self.record_warehouse_sync_anomaly(
                        now_ms,
                        AuditSeverity::Warning,
                        "warehouse_sync_degraded",
                        message.clone(),
                    )?;
                }
                eprintln!("{message}");
                self.warehouse_sync_degraded = true;
                Ok(())
            }
        }
    }

    fn record_warehouse_sync_anomaly(
        &mut self,
        timestamp_ms: u64,
        severity: AuditSeverity,
        code: &str,
        message: String,
    ) -> Result<()> {
        self.record_event(NewAuditEvent {
            timestamp_ms,
            kind: AuditEventKind::Anomaly,
            symbol: None,
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: Some(code.to_owned()),
            summary: message.clone(),
            payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                severity,
                code: code.to_owned(),
                message,
                symbol: None,
                signal_id: None,
                correlation_id: None,
            }),
        })
    }

    fn record_event(&mut self, event: NewAuditEvent) -> Result<()> {
        self.writer.append_event(event)?;
        Ok(())
    }
}

struct PaperAudit {
    sender: std_mpsc::Sender<AuditCommand>,
    worker_error: Arc<Mutex<Option<String>>>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
    session_id: String,
    session_dir: PathBuf,
    snapshot_interval_ms: u64,
    checkpoint_interval_ms: u64,
    warehouse_sync_interval_ms: u64,
    last_snapshot_ms: u64,
    last_checkpoint_ms: u64,
    last_summary_write_ms: u64,
    last_warehouse_sync_attempt_ms: u64,
}

impl PaperAudit {
    fn start(
        settings: &Settings,
        env: &str,
        markets: &[MarketConfig],
        now_ms: u64,
        trading_mode: polyalpha_core::TradingMode,
    ) -> Result<Self> {
        let manifest = AuditSessionManifest {
            version: 1,
            session_id: Uuid::new_v4().to_string(),
            env: env.to_owned(),
            mode: trading_mode,
            started_at_ms: now_ms,
            market_count: markets.len(),
            markets: markets
                .iter()
                .map(|market| market.symbol.0.clone())
                .collect(),
        };
        let mut writer = AuditWriter::create(
            &settings.general.data_dir,
            manifest.clone(),
            settings.audit.raw_segment_max_bytes,
        )?;
        let warehouse = AuditWarehouse::new(&settings.general.data_dir)?;
        let summary = AuditSessionSummary::new_running(&manifest);
        writer.write_summary(&summary)?;

        let session_id = summary.session_id.clone();
        let session_dir = writer.paths().session_dir().to_path_buf();
        let worker = PaperAuditWorker {
            data_dir: settings.general.data_dir.clone(),
            writer,
            warehouse,
            summary,
            warehouse_sync_degraded: false,
            last_mark_tick_index: None,
        };
        let (sender, receiver) = std_mpsc::channel();
        let worker_error = Arc::new(Mutex::new(None));
        let worker_error_clone = Arc::clone(&worker_error);
        let thread_name = format!("paper-audit-{session_id}");
        let worker_handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_paper_audit_worker(worker, receiver, worker_error_clone))
            .context("启动审计后台线程失败")?;

        Ok(Self {
            sender,
            worker_error,
            worker_handle: Some(worker_handle),
            session_id,
            session_dir,
            snapshot_interval_ms: settings.audit.snapshot_interval_secs.max(1) * 1_000,
            checkpoint_interval_ms: settings.audit.checkpoint_interval_secs.max(1) * 1_000,
            warehouse_sync_interval_ms: settings.audit.warehouse_sync_interval_secs.max(1) * 1_000,
            last_snapshot_ms: now_ms,
            last_checkpoint_ms: now_ms,
            last_summary_write_ms: now_ms,
            last_warehouse_sync_attempt_ms: now_ms,
        })
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn session_dir(&self) -> &std::path::Path {
        &self.session_dir
    }

    fn record_signal_emitted(
        &mut self,
        candidate: &OpenCandidate,
        observed: Option<AuditObservedMarket>,
    ) -> Result<()> {
        self.record_event(NewAuditEvent {
            timestamp_ms: candidate.timestamp_ms,
            kind: AuditEventKind::SignalEmitted,
            symbol: Some(candidate.symbol.0.clone()),
            signal_id: Some(candidate.candidate_id.clone()),
            correlation_id: Some(candidate.correlation_id.clone()),
            gate: None,
            result: None,
            reason: None,
            summary: summarize_candidate(candidate),
            payload: AuditEventPayload::SignalEmitted(SignalEmittedEvent {
                candidate: candidate.clone(),
                observed,
            }),
        })
    }

    fn record_gate_decision(
        &mut self,
        candidate: &OpenCandidate,
        gate: &str,
        result: AuditGateResult,
        reason: &str,
        summary: String,
        observed: Option<AuditObservedMarket>,
    ) -> Result<()> {
        self.record_gate_decision_with_diagnostics(
            candidate, gate, result, reason, summary, observed, None,
        )
    }

    fn record_gate_decision_with_diagnostics(
        &mut self,
        candidate: &OpenCandidate,
        gate: &str,
        result: AuditGateResult,
        reason: &str,
        summary: String,
        observed: Option<AuditObservedMarket>,
        planning_diagnostics: Option<PlanningDiagnostics>,
    ) -> Result<()> {
        self.record_event(NewAuditEvent {
            timestamp_ms: now_millis(),
            kind: AuditEventKind::GateDecision,
            symbol: Some(candidate.symbol.0.clone()),
            signal_id: Some(candidate.candidate_id.clone()),
            correlation_id: Some(candidate.correlation_id.clone()),
            gate: Some(gate.to_owned()),
            result: Some(result.as_str().to_owned()),
            reason: Some(reason.to_owned()),
            summary,
            payload: AuditEventPayload::GateDecision(GateDecisionEvent {
                gate: gate.to_owned(),
                result,
                reason: reason.to_owned(),
                candidate: candidate.clone(),
                observed,
                planning_diagnostics,
            }),
        })
    }

    fn record_evaluation_skip(
        &mut self,
        event: &MarketDataEvent,
        warnings: &[EngineWarning],
        observed: Option<AuditObservedMarket>,
    ) -> Result<()> {
        if warnings.is_empty() {
            return Ok(());
        }
        let primary_warning = primary_evaluation_skip_warning(warnings).map(|(_, warning)| warning);
        let summary = primary_warning
            .map(summarize_engine_warning)
            .unwrap_or_else(|| summarize_market_event(event));
        self.record_event(NewAuditEvent {
            timestamp_ms: now_millis(),
            kind: AuditEventKind::EvaluationSkip,
            symbol: warning_symbol_opt(primary_warning).map(|symbol| symbol.0.clone()),
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: None,
            summary,
            payload: AuditEventPayload::EvaluationSkip(EvaluationSkipEvent {
                source_event: summarize_market_event(event),
                symbol: warning_symbol_opt(primary_warning).map(|symbol| symbol.0.clone()),
                observed,
                warnings: warnings.to_vec(),
            }),
        })
    }

    fn record_execution(
        &mut self,
        event: &ExecutionEvent,
        trigger: Option<&RuntimeTrigger>,
    ) -> Result<()> {
        let (symbol, correlation_id) = audit_execution_identity(event, trigger);
        self.record_event(NewAuditEvent {
            timestamp_ms: audit_execution_timestamp_ms(event),
            kind: AuditEventKind::Execution,
            symbol,
            signal_id: trigger.map(|trigger| trigger.id().to_owned()),
            correlation_id,
            gate: None,
            result: None,
            reason: None,
            summary: summarize_execution_event(event),
            payload: AuditEventPayload::Execution(ExecutionAuditEvent {
                event: event.clone(),
            }),
        })
    }

    fn record_open_signal_rejected_post_submit(
        &mut self,
        candidate: &OpenCandidate,
        rejection_reasons: &[String],
    ) -> Result<()> {
        self.record_event(NewAuditEvent {
            timestamp_ms: now_millis(),
            kind: AuditEventKind::Anomaly,
            symbol: Some(candidate.symbol.0.clone()),
            signal_id: Some(candidate.candidate_id.clone()),
            correlation_id: Some(candidate.correlation_id.clone()),
            gate: None,
            result: None,
            reason: Some("post_submit_rejection".to_owned()),
            summary: format!(
                "候选提交后被拒绝 {}（{}）",
                candidate.candidate_id,
                rejection_reasons.join(", ")
            ),
            payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                severity: AuditSeverity::Warning,
                code: "post_submit_rejection".to_owned(),
                message: format!(
                    "开仓机会提交后被交易执行层拒绝：{}",
                    rejection_reasons.join(", ")
                ),
                symbol: Some(candidate.symbol.0.clone()),
                signal_id: Some(candidate.candidate_id.clone()),
                correlation_id: Some(candidate.correlation_id.clone()),
            }),
        })
    }

    fn record_intent_rejected(
        &mut self,
        trigger: &RuntimeTrigger,
        reason_code: &str,
        message: &str,
    ) -> Result<()> {
        self.record_event(NewAuditEvent {
            timestamp_ms: trigger.timestamp_ms(),
            kind: AuditEventKind::Anomaly,
            symbol: Some(trigger.symbol().0.clone()),
            signal_id: Some(trigger.id().to_owned()),
            correlation_id: Some(trigger.correlation_id().to_owned()),
            gate: Some("trade_plan".to_owned()),
            result: Some("rejected".to_owned()),
            reason: Some(reason_code.to_owned()),
            summary: format!(
                "{} {}未执行（{}）",
                trigger.symbol().0,
                trigger_action_label_zh(trigger),
                translate_trade_plan_rejection(reason_code)
            ),
            payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                severity: AuditSeverity::Warning,
                code: "intent_rejected".to_owned(),
                message: message.to_owned(),
                symbol: Some(trigger.symbol().0.clone()),
                signal_id: Some(trigger.id().to_owned()),
                correlation_id: Some(trigger.correlation_id().to_owned()),
            }),
        })
    }

    fn maybe_record_runtime_state(
        &mut self,
        stats: &PaperStats,
        monitor_state: &MonitorState,
        tick_index: usize,
        now_ms: u64,
    ) -> Result<()> {
        let record_marks = if now_ms.saturating_sub(self.last_snapshot_ms) >= self.snapshot_interval_ms
        {
            self.last_snapshot_ms = now_ms;
            true
        } else {
            false
        };

        let write_checkpoint =
            if now_ms.saturating_sub(self.last_checkpoint_ms) >= self.checkpoint_interval_ms {
                self.last_checkpoint_ms = now_ms;
                self.last_summary_write_ms = now_ms;
                true
            } else {
                false
            };

        let write_summary = if !write_checkpoint
            && now_ms.saturating_sub(self.last_summary_write_ms)
                >= AUDIT_SUMMARY_WRITE_THROTTLE_MS
        {
            self.last_summary_write_ms = now_ms;
            true
        } else {
            false
        };

        let sync_warehouse = if now_ms.saturating_sub(self.last_warehouse_sync_attempt_ms)
            >= self.warehouse_sync_interval_ms
        {
            self.last_warehouse_sync_attempt_ms = now_ms;
            true
        } else {
            false
        };

        if !(record_marks || write_checkpoint || write_summary || sync_warehouse) {
            return Ok(());
        }

        self.send_command(AuditCommand::RuntimeState {
            stats: stats.clone(),
            monitor_state: monitor_state.clone(),
            tick_index,
            now_ms,
            record_marks,
            write_checkpoint,
            write_summary,
            sync_warehouse,
        })
    }

    fn needs_runtime_state(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_snapshot_ms) >= self.snapshot_interval_ms
            || now_ms.saturating_sub(self.last_checkpoint_ms) >= self.checkpoint_interval_ms
            || now_ms.saturating_sub(self.last_summary_write_ms) >= AUDIT_SUMMARY_WRITE_THROTTLE_MS
    }

    fn finalize(
        &mut self,
        stats: &PaperStats,
        monitor_state: &MonitorState,
        tick_index: usize,
        now_ms: u64,
    ) -> Result<()> {
        self.check_worker_health()?;
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.sender
            .send(AuditCommand::Finalize {
                stats: stats.clone(),
                monitor_state: monitor_state.clone(),
                tick_index,
                now_ms,
                reply: reply_tx,
            })
            .map_err(|_| self.worker_closed_error())?;

        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(anyhow!("审计后台线程失败：{message}")),
            Err(err) => Err(anyhow!("等待审计后台线程完成失败：{err}")),
        }
    }

    #[cfg(test)]
    fn flush(&mut self) -> Result<()> {
        self.check_worker_health()?;
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.sender
            .send(AuditCommand::Barrier { reply: reply_tx })
            .map_err(|_| self.worker_closed_error())?;

        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(anyhow!("审计后台线程失败：{message}")),
            Err(err) => Err(anyhow!("等待审计后台线程刷新失败：{err}")),
        }
    }

    fn record_event(&mut self, event: NewAuditEvent) -> Result<()> {
        self.send_command(AuditCommand::Event(event))
    }

    fn send_command(&self, command: AuditCommand) -> Result<()> {
        self.check_worker_health()?;
        self.sender
            .send(command)
            .map_err(|_| self.worker_closed_error())
    }

    fn check_worker_health(&self) -> Result<()> {
        let worker_error = self
            .worker_error
            .lock()
            .map_err(|_| anyhow!("审计后台线程错误状态锁异常"))?;
        if let Some(message) = worker_error.as_ref() {
            Err(anyhow!("审计后台线程失败：{message}"))
        } else {
            Ok(())
        }
    }

    fn worker_closed_error(&self) -> anyhow::Error {
        self.worker_error
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .map(|message| anyhow!("审计后台线程失败：{message}"))
            .unwrap_or_else(|| anyhow!("审计后台线程已关闭"))
    }

    fn shutdown_worker(&mut self) {
        let Some(handle) = self.worker_handle.take() else {
            return;
        };
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        if self
            .sender
            .send(AuditCommand::Shutdown {
                reply: Some(reply_tx),
            })
            .is_ok()
        {
            let _ = reply_rx.recv();
        }
        let _ = handle.join();
    }
}

impl Drop for PaperAudit {
    fn drop(&mut self) {
        self.shutdown_worker();
    }
}

fn run_paper_audit_worker(
    mut worker: PaperAuditWorker,
    receiver: std_mpsc::Receiver<AuditCommand>,
    worker_error: Arc<Mutex<Option<String>>>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            AuditCommand::Event(event) => {
                if let Err(err) = worker.record_event(event) {
                    store_paper_audit_worker_error(&worker_error, &err);
                    break;
                }
            }
            AuditCommand::RuntimeState {
                stats,
                monitor_state,
                tick_index,
                now_ms,
                record_marks,
                write_checkpoint,
                write_summary,
                sync_warehouse,
            } => {
                if let Err(err) = worker.apply_runtime_state(
                    &stats,
                    &monitor_state,
                    tick_index,
                    now_ms,
                    record_marks,
                    write_checkpoint,
                    write_summary,
                    sync_warehouse,
                ) {
                    store_paper_audit_worker_error(&worker_error, &err);
                    break;
                }
            }
            AuditCommand::Finalize {
                stats,
                monitor_state,
                tick_index,
                now_ms,
                reply,
            } => {
                let result = worker
                    .finalize(&stats, &monitor_state, tick_index, now_ms)
                    .map_err(|err| format!("{err:#}"));
                if let Err(message) = &result {
                    if let Ok(mut guard) = worker_error.lock() {
                        *guard = Some(message.clone());
                    }
                }
                let _ = reply.send(result);
            }
            #[cfg(test)]
            AuditCommand::Barrier { reply } => {
                let result = worker.flush().map_err(|err| format!("{err:#}"));
                if let Err(message) = &result {
                    if let Ok(mut guard) = worker_error.lock() {
                        *guard = Some(message.clone());
                    }
                }
                let _ = reply.send(result);
            }
            AuditCommand::Shutdown { reply } => {
                let _ = worker.flush();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
                break;
            }
        }
    }

    let _ = worker.flush();
}

fn store_paper_audit_worker_error(
    worker_error: &Arc<Mutex<Option<String>>>,
    err: &anyhow::Error,
) {
    if let Ok(mut guard) = worker_error.lock() {
        if guard.is_none() {
            *guard = Some(format!("{err:#}"));
        }
    }
}

fn warehouse_sync_warning_message(err: &anyhow::Error) -> String {
    if is_warehouse_lock_conflict(err) {
        format!("审计仓库被其他进程占用，已保留原始审计文件：{err}")
    } else {
        format!("审计仓库同步失败，已保留原始审计文件：{err}")
    }
}

fn should_build_monitor_state(
    monitor_client_count: usize,
    audit: Option<&PaperAudit>,
    now_ms: u64,
) -> bool {
    monitor_client_count > 0 || audit.is_some_and(|audit| audit.needs_runtime_state(now_ms))
}

fn is_warehouse_lock_conflict(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("Could not set lock on file")
            || message.contains("Conflicting lock is held")
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaperEventLog {
    #[serde(default)]
    seq: u64,
    timestamp_ms: u64,
    kind: String,
    summary: String,
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    signal_id: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    details: Option<EventDetails>,
    #[serde(default)]
    payload: Option<MonitorEventPayload>,
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
    #[serde(deserialize_with = "deserialize_paper_schema_version")]
    schema_version: u32,
    #[serde(default = "default_artifact_trading_mode")]
    trading_mode: polyalpha_core::TradingMode,
    env: String,
    market_index: usize,
    market: String,
    hedge_exchange: String,
    #[serde(default)]
    market_data_mode: Option<String>,
    poll_interval_ms: u64,
    depth: u16,
    include_funding: bool,
    ticks_processed: usize,
    final_snapshot: PaperSnapshot,
    open_orders: Vec<PaperOrderView>,
    recent_events: Vec<PaperEventLog>,
    snapshots: Vec<PaperSnapshot>,
}

#[derive(Clone, Debug)]
struct RuntimeCommand {
    command_id: String,
    kind: CommandKind,
    status: CommandStatus,
    created_at_ms: u64,
    updated_at_ms: u64,
    finished: bool,
    timed_out: bool,
    cancellable: bool,
    message: Option<String>,
    error_code: Option<u32>,
}

impl RuntimeCommand {
    fn view(&self) -> CommandView {
        CommandView {
            command_id: self.command_id.clone(),
            kind: self.kind.clone(),
            status: self.status,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            finished: self.finished,
            timed_out: self.timed_out,
            cancellable: self.cancellable,
            message: self.message.clone(),
            error_code: self.error_code,
        }
    }
}

#[derive(Default)]
struct CommandCenter {
    order: VecDeque<String>,
    commands: HashMap<String, RuntimeCommand>,
    events: VecDeque<PaperEventLog>,
}

fn format_exchange_label(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Polymarket => "Polymarket",
        Exchange::Binance => "Binance",
        Exchange::Okx => "OKX",
    }
}

fn format_token_side_label(token_side: TokenSide) -> &'static str {
    match token_side {
        TokenSide::Yes => "YES",
        TokenSide::No => "NO",
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

fn market_phase_bucket_matches(left: &MarketPhase, right: &MarketPhase) -> bool {
    match (left, right) {
        (MarketPhase::Trading, MarketPhase::Trading)
        | (MarketPhase::PreSettlement { .. }, MarketPhase::PreSettlement { .. })
        | (MarketPhase::CloseOnly { .. }, MarketPhase::CloseOnly { .. })
        | (MarketPhase::SettlementPending, MarketPhase::SettlementPending)
        | (MarketPhase::Disputed, MarketPhase::Disputed)
        | (MarketPhase::Resolved, MarketPhase::Resolved) => true,
        (
            MarketPhase::ForceReduce {
                target_ratio: left_ratio,
                ..
            },
            MarketPhase::ForceReduce {
                target_ratio: right_ratio,
                ..
            },
        ) => left_ratio == right_ratio,
        _ => false,
    }
}

fn current_market_lifecycle_phase(
    market: &MarketConfig,
    now_timestamp_secs: u64,
    rules: &polyalpha_core::SettlementRules,
) -> MarketPhase {
    MarketPhase::from_settlement(market.settlement_timestamp, now_timestamp_secs, rules)
}

fn should_publish_market_lifecycle_phase(
    previous_phase: Option<&MarketPhase>,
    next_phase: &MarketPhase,
) -> bool {
    previous_phase.is_none_or(|phase| !market_phase_bucket_matches(phase, next_phase))
}

fn next_market_lifecycle_phase(
    observed: Option<&ObservedState>,
    market: &MarketConfig,
    now_timestamp_secs: u64,
    rules: &polyalpha_core::SettlementRules,
) -> Option<MarketPhase> {
    let next_phase = current_market_lifecycle_phase(market, now_timestamp_secs, rules);
    should_publish_market_lifecycle_phase(
        observed.and_then(|state| state.market_phase.as_ref()),
        &next_phase,
    )
    .then_some(next_phase)
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

fn format_command_status_label(status: CommandStatus) -> &'static str {
    match status {
        CommandStatus::Pending => "待处理",
        CommandStatus::Running => "执行中",
        CommandStatus::Success => "成功",
        CommandStatus::Failed => "失败",
        CommandStatus::PartialSuccess => "部分成功",
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

fn display_market_data_mode_text(value: &str) -> &str {
    if value.eq_ignore_ascii_case("ws") {
        "WS（推送）"
    } else if value.eq_ignore_ascii_case("poll") {
        "POLL（轮询）"
    } else {
        value
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
        "command" => "命令",
        "market_data" => "行情",
        "signal" => "信号",
        "fill" | "execution" => "成交",
        "trade_closed" => "平仓",
        "state" => "状态",
        "warning" => "警告",
        "skip" => "跳过",
        "risk" => "风控",
        "price_filter" => "价格过滤",
        "spread_filter" => "点差过滤",
        "paused" => "已暂停",
        "emergency" => "紧急停止",
        "stale" => "行情超时",
        "reconcile" => "对账",
        "error" | "poll_error" => "错误",
        "market_closed" => "休市",
        "emergency_fill" => "紧急成交",
        "emergency_cex_close" => "紧急平仓",
        "emergency_close_failed" => "紧急平仓失败",
        _ => value,
    }
}

fn describe_config_update_zh(config: &polyalpha_core::ConfigUpdate) -> String {
    if let Some(value) = config.entry_z {
        format!("入场Z={value:.2}")
    } else if let Some(value) = config.exit_z {
        format!("出场Z={value:.2}")
    } else if let Some(value) = config.position_notional_usd {
        format!("单次仓位=${value:.0}")
    } else if let Some(value) = config.rolling_window {
        format!("滚动窗口={}s", value)
    } else if let Some(value) = config.band_policy.as_deref() {
        format!("价格带策略={value}")
    } else if let Some(value) = config.min_poly_price {
        format!("最小Polymarket价格={value:.3}")
    } else if let Some(value) = config.max_poly_price {
        format!("最大Polymarket价格={value:.3}")
    } else {
        "未指定参数".to_owned()
    }
}

fn describe_command_kind_zh(kind: &CommandKind) -> String {
    match kind {
        CommandKind::Pause => "暂停交易".to_owned(),
        CommandKind::Resume => "恢复交易".to_owned(),
        CommandKind::ClosePosition { market } => format!("平仓({market})"),
        CommandKind::CloseAllPositions => "全部平仓".to_owned(),
        CommandKind::EmergencyStop => "紧急停止".to_owned(),
        CommandKind::ClearEmergency => "解除紧急停止".to_owned(),
        CommandKind::UpdateConfig { config } => {
            format!("调整参数({})", describe_config_update_zh(config))
        }
        CommandKind::CancelCommand { command_id } => format!("取消命令({command_id})"),
    }
}

impl CommandCenter {
    fn enqueue(&mut self, command_id: String, kind: CommandKind, now_ms: u64) -> RuntimeCommand {
        let command = RuntimeCommand {
            command_id: command_id.clone(),
            kind: kind.clone(),
            status: CommandStatus::Pending,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished: false,
            timed_out: false,
            cancellable: true,
            message: Some("命令已排队".to_owned()),
            error_code: None,
        };
        self.commands.insert(command_id.clone(), command.clone());
        self.order.push_back(command_id.clone());
        self.push_event(
            now_ms,
            "command",
            format!("命令已排队：{}", describe_command_kind_zh(&kind)),
            Some(command_id),
            build_command_event_details(
                &command.command_id,
                &command.kind,
                Some(command.status),
                command.message.as_deref(),
                command.error_code,
            ),
        );
        self.trim();
        command
    }

    fn cancel(
        &mut self,
        cancel_command_id: String,
        target_command_id: String,
        now_ms: u64,
    ) -> RuntimeCommand {
        let mut cancel_command = RuntimeCommand {
            command_id: cancel_command_id.clone(),
            kind: CommandKind::CancelCommand {
                command_id: target_command_id.clone(),
            },
            status: CommandStatus::Pending,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished: false,
            timed_out: false,
            cancellable: false,
            message: None,
            error_code: None,
        };

        let outcome = match self.commands.get_mut(&target_command_id) {
            Some(target) if !target.finished && target.cancellable => {
                target.status = CommandStatus::Failed;
                target.updated_at_ms = now_ms;
                target.finished = true;
                target.cancellable = false;
                target.message = Some("命令已取消".to_owned());
                target.error_code = Some(1004);
                let details = build_command_event_details(
                    &target.command_id,
                    &target.kind,
                    Some(target.status),
                    target.message.as_deref(),
                    target.error_code,
                );
                self.push_event(
                    now_ms,
                    "command",
                    format!("命令已取消：{target_command_id}"),
                    Some(target_command_id.clone()),
                    details,
                );
                Ok("取消成功".to_owned())
            }
            Some(_) => Err(("目标命令不可取消".to_owned(), 1005_u32)),
            None => Err(("目标命令不存在".to_owned(), 1005_u32)),
        };

        match outcome {
            Ok(message) => {
                cancel_command.status = CommandStatus::Success;
                cancel_command.finished = true;
                cancel_command.message = Some(message.clone());
                self.push_event(
                    now_ms,
                    "command",
                    format!("取消命令成功：{target_command_id}"),
                    Some(cancel_command_id.clone()),
                    build_command_event_details(
                        &cancel_command.command_id,
                        &cancel_command.kind,
                        Some(cancel_command.status),
                        cancel_command.message.as_deref(),
                        cancel_command.error_code,
                    ),
                );
            }
            Err((message, error_code)) => {
                cancel_command.status = CommandStatus::Failed;
                cancel_command.finished = true;
                cancel_command.message = Some(message.clone());
                cancel_command.error_code = Some(error_code);
                self.push_event(
                    now_ms,
                    "command",
                    format!("取消命令失败：{target_command_id}，{message}"),
                    Some(cancel_command_id.clone()),
                    build_command_event_details(
                        &cancel_command.command_id,
                        &cancel_command.kind,
                        Some(cancel_command.status),
                        cancel_command.message.as_deref(),
                        cancel_command.error_code,
                    ),
                );
            }
        }

        self.commands
            .insert(cancel_command_id.clone(), cancel_command.clone());
        self.order.push_back(cancel_command_id);
        self.trim();
        cancel_command
    }

    fn start_next(&mut self, now_ms: u64) -> Option<RuntimeCommand> {
        let next_id = self.order.iter().find_map(|id| {
            self.commands.get(id).and_then(|command| {
                if command.status == CommandStatus::Pending && !command.finished {
                    Some(id.clone())
                } else {
                    None
                }
            })
        })?;

        let command = self.commands.get_mut(&next_id)?;
        command.status = CommandStatus::Running;
        command.updated_at_ms = now_ms;
        command.cancellable = false;
        command.message = Some("命令执行中".to_owned());
        let kind = command.kind.clone();
        let command = command.clone();
        self.push_event(
            now_ms,
            "command",
            format!("开始执行命令：{}", describe_command_kind_zh(&kind)),
            Some(next_id.clone()),
            build_command_event_details(
                &command.command_id,
                &command.kind,
                Some(command.status),
                command.message.as_deref(),
                command.error_code,
            ),
        );
        Some(command)
    }

    fn finish(
        &mut self,
        command_id: &str,
        now_ms: u64,
        status: CommandStatus,
        message: Option<String>,
        error_code: Option<u32>,
    ) {
        if let Some(command) = self.commands.get_mut(command_id) {
            command.status = status;
            command.updated_at_ms = now_ms;
            command.finished = true;
            command.cancellable = false;
            command.message = message.clone();
            command.error_code = error_code;
            let details = build_command_event_details(
                &command.command_id,
                &command.kind,
                Some(command.status),
                command.message.as_deref(),
                command.error_code,
            );
            self.push_event(
                now_ms,
                "command",
                format!(
                    "命令 {} {}：{}",
                    command_id,
                    format_command_status_label(status),
                    message.clone().unwrap_or_else(|| "已完成".to_owned())
                ),
                Some(command_id.to_owned()),
                details,
            );
        }
        self.trim();
    }

    fn recent_commands(&self) -> Vec<CommandView> {
        self.order
            .iter()
            .rev()
            .filter_map(|id| self.commands.get(id))
            .take(MAX_RECENT_COMMANDS)
            .map(RuntimeCommand::view)
            .collect()
    }

    fn drain_events(&mut self) -> Vec<PaperEventLog> {
        self.events.drain(..).collect()
    }

    fn ack_message(command: &RuntimeCommand) -> polyalpha_core::Message {
        polyalpha_core::Message::CommandAck {
            command_id: command.command_id.clone(),
            kind: command.kind.clone(),
            status: command.status,
            success: matches!(
                command.status,
                CommandStatus::Pending | CommandStatus::Running | CommandStatus::Success
            ),
            message: command.message.clone(),
            error_code: command.error_code,
            finished: command.finished,
            timed_out: command.timed_out,
            cancellable: command.cancellable,
        }
    }

    fn push_event(
        &mut self,
        timestamp_ms: u64,
        kind: &str,
        summary: String,
        correlation_id: Option<String>,
        details: Option<EventDetails>,
    ) {
        if self.events.len() == MAX_EVENT_LOGS {
            self.events.pop_front();
        }
        self.events.push_back(PaperEventLog {
            seq: 0,
            timestamp_ms,
            kind: kind.to_owned(),
            summary,
            market: None,
            signal_id: None,
            correlation_id,
            details,
            payload: None,
        });
    }

    fn trim(&mut self) {
        while self.order.len() > MAX_RECENT_COMMANDS * 2 {
            let Some(idx) = self.order.iter().position(|id| {
                self.commands
                    .get(id)
                    .map(|command| command.finished)
                    .unwrap_or(true)
            }) else {
                break;
            };

            if let Some(oldest) = self.order.remove(idx) {
                self.commands.remove(&oldest);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct OpenTradeInfo {
    market: String,
    direction: String,
    token_side: TokenSide,
    opened_at_ms: u64,
    basis_entry_bps: i32,
    delta: f64,
    hedge_ratio: f64,
    last_planned_cex_qty: Option<Decimal>,
    correlation_id: String,
    poly_entry_price: Option<f64>,
    poly_exit_price: Option<f64>,
    cex_entry_price: Option<f64>,
    cex_exit_price: Option<f64>,
}

impl Default for OpenTradeInfo {
    fn default() -> Self {
        Self {
            market: String::new(),
            direction: String::new(),
            token_side: TokenSide::Yes,
            opened_at_ms: 0,
            basis_entry_bps: 0,
            delta: 0.0,
            hedge_ratio: 1.0,
            last_planned_cex_qty: None,
            correlation_id: String::new(),
            poly_entry_price: None,
            poly_exit_price: None,
            cex_entry_price: None,
            cex_exit_price: None,
        }
    }
}

#[derive(Clone, Debug)]
struct ClosedTradeInfo {
    market: String,
    opened_at_ms: u64,
    closed_at_ms: u64,
    direction: String,
    token_side: String,
    entry_price: Option<f64>,
    exit_price: Option<f64>,
    realized_pnl_usd: f64,
    correlation_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct EquityPoint {
    timestamp_ms: u64,
    equity: f64,
}

#[derive(Clone, Default)]
struct TradeBook {
    open: HashMap<Symbol, OpenTradeInfo>,
    closed: VecDeque<ClosedTradeInfo>,
    equity_curve: VecDeque<EquityPoint>,
}

impl TradeBook {
    fn register_open_signal(
        &mut self,
        candidate: &OpenCandidate,
        basis_entry_bps: i32,
        hedge_ratio: f64,
        delta: f64,
    ) {
        self.open.insert(
            candidate.symbol.clone(),
            OpenTradeInfo {
                market: candidate.symbol.0.clone(),
                direction: candidate_direction_zh(candidate.direction.as_str()).to_owned(),
                token_side: candidate.token_side,
                opened_at_ms: 0,
                basis_entry_bps,
                delta,
                hedge_ratio,
                correlation_id: candidate.correlation_id.clone(),
                ..OpenTradeInfo::default()
            },
        );
    }

    fn discard_open_trade(&mut self, symbol: &Symbol) {
        self.open.remove(symbol);
    }

    fn record_trade_plan(&mut self, plan: &TradePlan) {
        if !matches!(
            plan.intent_type.as_str(),
            "open_position" | "delta_rebalance" | "residual_recovery"
        ) {
            return;
        }
        if let Some(open) = self.open.get_mut(&plan.symbol) {
            open.last_planned_cex_qty = Some(plan.cex_planned_qty.0);
        }
    }

    fn record_fill(&mut self, trigger: Option<&RuntimeTrigger>, fill: &Fill) {
        let Some(trigger) = trigger else {
            return;
        };
        let Some(open) = self.open.get_mut(&fill.symbol) else {
            return;
        };

        if open.opened_at_ms == 0 && trigger.candidate().is_some() {
            open.opened_at_ms = fill.timestamp_ms;
        }

        match (
            trigger.candidate().is_some(),
            trigger.is_close_like(),
            fill.instrument,
        ) {
            (true, _, InstrumentKind::PolyYes | InstrumentKind::PolyNo) => {
                open.poly_entry_price
                    .get_or_insert(fill.price.0.to_f64().unwrap_or_default());
            }
            (true, _, InstrumentKind::CexPerp) => {
                open.cex_entry_price
                    .get_or_insert(fill.price.0.to_f64().unwrap_or_default());
            }
            (_, true, InstrumentKind::PolyYes | InstrumentKind::PolyNo) => {
                open.poly_exit_price = Some(fill.price.0.to_f64().unwrap_or_default());
            }
            (_, true, InstrumentKind::CexPerp) => {
                open.cex_exit_price = Some(fill.price.0.to_f64().unwrap_or_default());
            }
            _ => {}
        }
    }

    fn close_trade(
        &mut self,
        symbol: &Symbol,
        closed_at_ms: u64,
        realized_pnl_usd: f64,
        correlation_id: Option<String>,
    ) {
        let Some(open) = self.open.remove(symbol) else {
            return;
        };
        if self.closed.len() == MAX_RECENT_TRADES {
            self.closed.pop_front();
        }
        self.closed.push_back(ClosedTradeInfo {
            market: open.market,
            opened_at_ms: open.opened_at_ms,
            closed_at_ms,
            direction: open.direction,
            token_side: format!("{:?}", open.token_side),
            entry_price: open.poly_entry_price,
            exit_price: open.poly_exit_price,
            realized_pnl_usd,
            correlation_id: correlation_id.or(Some(open.correlation_id)),
        });
    }

    fn recent_trades(&self) -> Vec<TradeView> {
        self.closed
            .iter()
            .rev()
            .take(MAX_RECENT_TRADES)
            .map(|trade| TradeView {
                market: trade.market.clone(),
                opened_at_ms: trade.opened_at_ms,
                closed_at_ms: trade.closed_at_ms,
                direction: trade.direction.clone(),
                token_side: trade.token_side.clone(),
                entry_price: trade.entry_price,
                exit_price: trade.exit_price,
                realized_pnl_usd: trade.realized_pnl_usd,
                correlation_id: trade.correlation_id.clone(),
            })
            .collect()
    }

    fn win_rate_pct(&self) -> f64 {
        if self.closed.is_empty() {
            return 0.0;
        }
        let wins = self
            .closed
            .iter()
            .filter(|trade| trade.realized_pnl_usd > 0.0)
            .count();
        wins as f64 * 100.0 / self.closed.len() as f64
    }

    fn profit_factor(&self) -> f64 {
        let gross_profit: f64 = self
            .closed
            .iter()
            .filter(|trade| trade.realized_pnl_usd > 0.0)
            .map(|trade| trade.realized_pnl_usd)
            .sum();
        let gross_loss: f64 = self
            .closed
            .iter()
            .filter(|trade| trade.realized_pnl_usd < 0.0)
            .map(|trade| trade.realized_pnl_usd.abs())
            .sum();
        if gross_loss <= f64::EPSILON {
            if gross_profit > 0.0 {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            gross_profit / gross_loss
        }
    }

    fn push_equity(&mut self, timestamp_ms: u64, equity: f64) {
        if self.equity_curve.len() == MAX_EQUITY_POINTS {
            self.equity_curve.pop_front();
        }
        self.equity_curve.push_back(EquityPoint {
            timestamp_ms,
            equity,
        });
    }

    fn max_drawdown_pct(&self) -> f64 {
        let mut peak = f64::MIN;
        let mut max_drawdown: f64 = 0.0;
        for point in &self.equity_curve {
            peak = peak.max(point.equity);
            if peak > 0.0 {
                max_drawdown = max_drawdown.max((peak - point.equity) / peak);
            }
        }
        max_drawdown * 100.0
    }

    fn today_pnl_usd(&self, current_equity: f64, initial_capital: f64, now_ms: u64) -> f64 {
        let now_date = local_day_key(now_ms);
        let first_today_equity = self
            .equity_curve
            .iter()
            .find(|point| local_day_key(point.timestamp_ms) == now_date)
            .map(|point| point.equity);
        current_equity - first_today_equity.unwrap_or(initial_capital)
    }
}

#[derive(Clone)]
enum CexLiveSource {
    Binance(BinanceFuturesDataSource),
    Okx(OkxMarketDataSource),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FundingRefreshTarget {
    exchange: Exchange,
    venue_symbol: String,
}

#[derive(Debug)]
struct FundingRefreshResult {
    total_targets: usize,
    failed_targets: usize,
    poll_errors: usize,
    duration_ms: u64,
    target_errors: Vec<String>,
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

    async fn fetch_klines(&self, symbol: &Symbol, limit: u16) -> Result<Vec<(u64, f64)>> {
        match self {
            Self::Binance(source) => Ok(source
                .fetch_klines_by_symbol(symbol, limit)
                .await?
                .into_iter()
                .map(|item| (item.open_time_ms, item.close))
                .collect()),
            Self::Okx(_) => Ok(Vec::new()),
        }
    }

    async fn poll_funding_for_venue_symbol(&self, venue_symbol: &str) -> Result<usize> {
        match self {
            Self::Binance(source) => {
                source
                    .fetch_and_publish_funding_and_mark(venue_symbol)
                    .await
            }
            Self::Okx(source) => {
                source
                    .fetch_and_publish_funding_and_mark(venue_symbol)
                    .await
            }
        }
        .map_err(Into::into)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MultiMarketBootstrapPhase {
    Startup,
    SnapshotRefresh,
}

impl MultiMarketBootstrapPhase {
    fn label_zh(self) -> &'static str {
        match self {
            Self::Startup => "启动补齐",
            Self::SnapshotRefresh => "后台重同步",
        }
    }
}

#[derive(Debug)]
struct MultiMarketBootstrapProgress {
    phase: MultiMarketBootstrapPhase,
    completed_markets: usize,
    total_markets: usize,
    poll_errors: usize,
    skipped_poly_markets: usize,
    duration_ms: Option<u64>,
}

impl MultiMarketBootstrapProgress {
    fn started(phase: MultiMarketBootstrapPhase, total_markets: usize) -> Self {
        Self {
            phase,
            completed_markets: 0,
            total_markets,
            poll_errors: 0,
            skipped_poly_markets: 0,
            duration_ms: None,
        }
    }
}

#[derive(Debug, Default)]
struct MultiMarketBootstrapMarketResult {
    poll_errors: usize,
    skipped_poly_market: bool,
}

fn multi_market_bootstrap_concurrency(total_markets: usize) -> usize {
    total_markets
        .clamp(1, MULTI_MARKET_BOOTSTRAP_MAX_CONCURRENCY)
        .max(1)
}

fn multi_market_bootstrap_progress_step(total_markets: usize) -> usize {
    (total_markets / 8).max(25)
}

fn multi_market_funding_concurrency(total_targets: usize) -> usize {
    total_targets
        .clamp(1, MULTI_MARKET_FUNDING_MAX_CONCURRENCY)
        .max(1)
}

fn funding_target_for_market(market: &MarketConfig) -> FundingRefreshTarget {
    FundingRefreshTarget {
        exchange: market.hedge_exchange,
        venue_symbol: market.cex_symbol.clone(),
    }
}

fn collect_unique_funding_targets(markets: &[MarketConfig]) -> Vec<FundingRefreshTarget> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();

    for market in markets {
        let key = (market.hedge_exchange, market.cex_symbol.clone());
        if seen.insert(key.clone()) {
            targets.push(FundingRefreshTarget {
                exchange: key.0,
                venue_symbol: key.1,
            });
        }
    }

    targets.sort_by(|left, right| {
        funding_exchange_rank(left.exchange)
            .cmp(&funding_exchange_rank(right.exchange))
            .then_with(|| left.venue_symbol.cmp(&right.venue_symbol))
    });
    targets
}

fn funding_exchange_rank(exchange: Exchange) -> usize {
    match exchange {
        Exchange::Binance => 0,
        Exchange::Okx => 1,
        Exchange::Polymarket => 2,
    }
}

fn funding_target_label(target: &FundingRefreshTarget) -> String {
    format!("{:?}/{}", target.exchange, target.venue_symbol)
}

fn spawn_funding_refresh(
    cex_sources: HashMap<Exchange, CexLiveSource>,
    targets: Vec<FundingRefreshTarget>,
    tx: mpsc::UnboundedSender<FundingRefreshResult>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started_at_ms = now_millis();
        let total_targets = targets.len();
        let mut results = futures_util::stream::iter(targets.into_iter().map(|target| {
            let cex_sources = cex_sources.clone();
            async move {
                let result = match cex_sources.get(&target.exchange) {
                    Some(source) => {
                        source
                            .poll_funding_for_venue_symbol(&target.venue_symbol)
                            .await
                    }
                    None => Err(anyhow!(
                        "未找到 {:?} 的行情源，无法刷新 {}",
                        target.exchange,
                        target.venue_symbol
                    )),
                };
                (target, result)
            }
        }))
        .buffer_unordered(multi_market_funding_concurrency(total_targets.max(1)))
        .collect::<Vec<_>>()
        .await;

        results.sort_by(|left, right| {
            funding_target_label(&left.0).cmp(&funding_target_label(&right.0))
        });

        let mut failed_targets = 0usize;
        let mut poll_errors = 0usize;
        let mut target_errors = Vec::new();
        for (target, result) in results {
            if let Err(err) = result {
                failed_targets += 1;
                poll_errors += 1;
                target_errors.push(format!(
                    "{} 资金费刷新失败：{}",
                    funding_target_label(&target),
                    err
                ));
            }
        }

        let _ = tx.send(FundingRefreshResult {
            total_targets,
            failed_targets,
            poll_errors,
            duration_ms: now_millis().saturating_sub(started_at_ms),
            target_errors,
        });
    })
}

async fn bootstrap_multi_market_snapshot_market(
    market: MarketConfig,
    poly_source: PolymarketLiveDataSource,
    cex_sources: HashMap<Exchange, CexLiveSource>,
    depth: u16,
    include_funding: bool,
) -> MultiMarketBootstrapMarketResult {
    let mut result = MultiMarketBootstrapMarketResult::default();

    if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
        || looks_like_placeholder_id(&market.poly_ids.no_token_id)
    {
        result.skipped_poly_market = true;
    } else {
        for token_side in [TokenSide::Yes, TokenSide::No] {
            if poly_source
                .fetch_and_publish_orderbook_by_symbol(&market.symbol, token_side)
                .await
                .is_err()
            {
                result.poll_errors += 1;
            }
        }
    }

    if let Some(source) = cex_sources.get(&market.hedge_exchange) {
        if source
            .poll_symbol(&market.symbol, depth, include_funding)
            .await
            .is_err()
        {
            result.poll_errors += 1;
        }
    }

    result
}

fn spawn_multi_market_snapshot_bootstrap(
    phase: MultiMarketBootstrapPhase,
    markets: Vec<MarketConfig>,
    poly_source: PolymarketLiveDataSource,
    cex_sources: HashMap<Exchange, CexLiveSource>,
    depth: u16,
    include_funding: bool,
    progress_tx: mpsc::UnboundedSender<MultiMarketBootstrapProgress>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let total_markets = markets.len();
        let started_at_ms = now_millis();
        let progress_step = multi_market_bootstrap_progress_step(total_markets);
        let concurrency = multi_market_bootstrap_concurrency(total_markets);
        let _ = progress_tx.send(MultiMarketBootstrapProgress::started(phase, total_markets));

        let tasks = markets.into_iter().map(|market| {
            let poly_source = poly_source.clone();
            let cex_sources = cex_sources.clone();
            async move {
                bootstrap_multi_market_snapshot_market(
                    market,
                    poly_source,
                    cex_sources,
                    depth,
                    include_funding,
                )
                .await
            }
        });

        let mut completed_markets = 0usize;
        let mut poll_errors = 0usize;
        let mut skipped_poly_markets = 0usize;
        let mut last_progress_markets = 0usize;
        let mut stream = futures_util::stream::iter(tasks).buffer_unordered(concurrency);

        while let Some(result) = stream.next().await {
            completed_markets += 1;
            poll_errors += result.poll_errors;
            skipped_poly_markets += usize::from(result.skipped_poly_market);

            if completed_markets == total_markets
                || completed_markets.saturating_sub(last_progress_markets) >= progress_step
            {
                last_progress_markets = completed_markets;
                let _ = progress_tx.send(MultiMarketBootstrapProgress {
                    phase,
                    completed_markets,
                    total_markets,
                    poll_errors,
                    skipped_poly_markets,
                    duration_ms: None,
                });
            }
        }

        let _ = progress_tx.send(MultiMarketBootstrapProgress {
            phase,
            completed_markets,
            total_markets,
            poll_errors,
            skipped_poly_markets,
            duration_ms: Some(now_millis().saturating_sub(started_at_ms)),
        });
    })
}

async fn warmup_market_engine(
    market: &MarketConfig,
    engine: &mut SimpleAlphaEngine,
    cex_source: &CexLiveSource,
    poly_source: &PolymarketLiveDataSource,
    warmup_klines: u16,
) -> Result<()> {
    let data = fetch_market_warmup_data(market, cex_source, poly_source, warmup_klines).await?;
    apply_market_warmup_data(market, engine, &data);
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct MarketWarmupData {
    cex_klines: Vec<(u64, f64)>,
    poly_histories: Vec<(TokenSide, Vec<(u64, f64)>)>,
    poly_history_errors: Vec<(TokenSide, String)>,
}

#[derive(Debug)]
enum MultiMarketWarmupEvent {
    Started {
        total_markets: usize,
        concurrency: usize,
    },
    MarketData {
        market: MarketConfig,
        data: MarketWarmupData,
    },
    MarketFailed {
        symbol: String,
        error: String,
    },
    Finished {
        total_markets: usize,
        failed_markets: usize,
        duration_ms: u64,
    },
}

fn multi_market_warmup_concurrency(total_markets: usize) -> usize {
    total_markets
        .clamp(1, MULTI_MARKET_WARMUP_MAX_CONCURRENCY)
        .max(1)
}

fn multi_market_warmup_progress_step(total_markets: usize) -> usize {
    (total_markets / 8).max(25)
}

fn warmup_history_fetch_limit(warmup_klines: u16) -> u16 {
    if warmup_klines == 0 {
        return 0;
    }

    let proportional_headroom = (warmup_klines / 4).max(MIN_WARMUP_FETCH_HEADROOM);
    warmup_klines
        .saturating_add(proportional_headroom)
        .min(MAX_WARMUP_FETCH_KLINES)
}

async fn fetch_market_warmup_data(
    market: &MarketConfig,
    cex_source: &CexLiveSource,
    poly_source: &PolymarketLiveDataSource,
    warmup_klines: u16,
) -> Result<MarketWarmupData> {
    if warmup_klines == 0 {
        return Ok(MarketWarmupData::default());
    }

    let fetch_limit = warmup_history_fetch_limit(warmup_klines);
    let cex_klines = cex_source.fetch_klines(&market.symbol, fetch_limit).await?;
    if cex_klines.is_empty() {
        return Ok(MarketWarmupData::default());
    }

    if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
        || looks_like_placeholder_id(&market.poly_ids.no_token_id)
    {
        return Ok(MarketWarmupData {
            cex_klines,
            poly_histories: Vec::new(),
            poly_history_errors: Vec::new(),
        });
    }

    let start_ts = cex_klines
        .first()
        .map(|(ts, _)| ts / 1000)
        .unwrap_or_default();
    let end_ts = cex_klines
        .last()
        .map(|(ts, _)| ts / 1000)
        .unwrap_or(start_ts);

    let mut poly_histories = Vec::new();
    let mut poly_history_errors = Vec::new();
    for token_side in [TokenSide::Yes, TokenSide::No] {
        match poly_source
            .fetch_price_history_by_symbol(&market.symbol, token_side, start_ts, end_ts)
            .await
        {
            Ok(history) => {
                if history.is_empty() {
                    continue;
                }
                let prices = history
                    .into_iter()
                    .map(|point| (point.ts_ms, point.price))
                    .collect::<Vec<_>>();
                poly_histories.push((token_side, prices));
            }
            Err(error) => poly_history_errors.push((token_side, error.to_string())),
        }
    }

    Ok(MarketWarmupData {
        cex_klines,
        poly_histories,
        poly_history_errors,
    })
}

fn warmup_token_history_len(data: &MarketWarmupData, token_side: TokenSide) -> usize {
    data.poly_histories
        .iter()
        .find_map(|(side, prices)| (*side == token_side).then_some(prices.len()))
        .unwrap_or(0)
}

fn market_warmup_diagnostic_message(
    market: &MarketConfig,
    data: &MarketWarmupData,
) -> Option<String> {
    if data.cex_klines.is_empty() {
        return None;
    }
    if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
        || looks_like_placeholder_id(&market.poly_ids.no_token_id)
    {
        return None;
    }

    let yes_history_len = warmup_token_history_len(data, TokenSide::Yes);
    let no_history_len = warmup_token_history_len(data, TokenSide::No);
    let has_missing_side = yes_history_len == 0 || no_history_len == 0;
    let has_errors = !data.poly_history_errors.is_empty();
    if !has_missing_side && !has_errors {
        return None;
    }

    let mut message = format!(
        "{} warmup 异常：cex={} yes={} no={}",
        market.symbol.0,
        data.cex_klines.len(),
        yes_history_len,
        no_history_len,
    );
    if has_errors {
        let errors = data
            .poly_history_errors
            .iter()
            .map(|(token_side, error)| {
                format!("{}={}", format_token_side_label(*token_side), error)
            })
            .collect::<Vec<_>>()
            .join(", ");
        message.push_str(&format!(" errors: {errors}"));
    }
    Some(message)
}

fn market_warmup_status(data: &MarketWarmupData) -> MarketWarmupStatus {
    MarketWarmupStatus {
        cex_klines: data.cex_klines.len(),
        yes_history_len: warmup_token_history_len(data, TokenSide::Yes),
        no_history_len: warmup_token_history_len(data, TokenSide::No),
        cex_fetch_error: None,
        poly_history_errors: data.poly_history_errors.clone(),
    }
}

fn apply_market_warmup_data(
    market: &MarketConfig,
    engine: &mut SimpleAlphaEngine,
    data: &MarketWarmupData,
) {
    if data.cex_klines.is_empty() {
        return;
    }

    engine.warmup_cex_prices(&market.symbol, &data.cex_klines);
    if data.poly_histories.is_empty() {
        return;
    }

    let cex_prices = data
        .cex_klines
        .iter()
        .map(|(timestamp_ms, price)| (*timestamp_ms, *price))
        .collect::<HashMap<_, _>>();
    for (token_side, prices) in &data.poly_histories {
        engine.warmup_poly_prices(&market.symbol, *token_side, market, &cex_prices, prices);
    }
}

fn build_strategy_health_summary(
    markets: &[MarketView],
    warmup_status: &HashMap<Symbol, MarketWarmupStatus>,
    min_warmup_samples: usize,
    warmup_total_markets: usize,
    warmup_completed_markets: usize,
    warmup_failed_markets: usize,
) -> StrategyHealthView {
    let mut summary = StrategyHealthView {
        total_markets: markets.len() as u64,
        min_warmup_samples: min_warmup_samples as u64,
        warmup_total_markets: warmup_total_markets as u64,
        warmup_completed_markets: warmup_completed_markets as u64,
        warmup_failed_markets: warmup_failed_markets as u64,
        ..StrategyHealthView::default()
    };

    for market in markets {
        if market.fair_value.is_some() {
            summary.fair_value_ready_markets += 1;
        }
        if market.z_score.is_some() {
            summary.z_score_ready_markets += 1;
        }
        if market.basis_history_len >= min_warmup_samples && market.basis_history_len > 0 {
            summary.warmup_ready_markets += 1;
        } else if market.fair_value.is_some() || market.basis_history_len > 0 {
            summary.insufficient_warmup_markets += 1;
        }
        if matches!(
            market.evaluable_status,
            EvaluableStatus::PolyQuoteStale | EvaluableStatus::CexQuoteStale
        ) {
            summary.stale_markets += 1;
        }
        if matches!(market.evaluable_status, EvaluableStatus::NotEvaluableNoPoly)
            || matches!(
                market.async_classification,
                AsyncClassification::NoPolySample
            )
        {
            summary.no_poly_markets += 1;
        }
    }

    summary.cex_history_failed_markets = warmup_status
        .values()
        .filter(|status| status.cex_fetch_error.is_some())
        .count() as u64;
    summary.poly_history_failed_markets = warmup_status
        .values()
        .filter(|status| !status.poly_history_errors.is_empty())
        .count() as u64;

    summary
}

fn strategy_health_event_details(summary: &StrategyHealthView) -> EventDetails {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "市场总数", summary.total_markets.to_string());
    insert_detail(
        &mut details,
        "公允值就绪市场数",
        summary.fair_value_ready_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "Z值就绪市场数",
        summary.z_score_ready_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "历史预热完成市场数",
        summary.warmup_ready_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "预热不足市场数",
        summary.insufficient_warmup_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "Stale市场数",
        summary.stale_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "NoPoly市场数",
        summary.no_poly_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "CEX历史失败市场数",
        summary.cex_history_failed_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "Poly历史失败市场数",
        summary.poly_history_failed_markets.to_string(),
    );
    insert_detail(
        &mut details,
        "预热门槛样本数",
        summary.min_warmup_samples.to_string(),
    );
    insert_detail(
        &mut details,
        "后台预热进度",
        format!(
            "{}/{}",
            summary.warmup_completed_markets, summary.warmup_total_markets
        ),
    );
    insert_detail(
        &mut details,
        "后台预热失败市场数",
        summary.warmup_failed_markets.to_string(),
    );
    details
}

fn market_warmup_event_details(symbol: &str, status: &MarketWarmupStatus) -> EventDetails {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "市场", symbol.to_owned());
    insert_detail(&mut details, "CEX历史样本数", status.cex_klines.to_string());
    insert_detail(
        &mut details,
        "YES历史样本数",
        status.yes_history_len.to_string(),
    );
    insert_detail(
        &mut details,
        "NO历史样本数",
        status.no_history_len.to_string(),
    );
    if let Some(error) = status.cex_fetch_error.as_ref() {
        insert_detail(&mut details, "CEX历史抓取错误", error.clone());
    }
    if !status.poly_history_errors.is_empty() {
        let errors = status
            .poly_history_errors
            .iter()
            .map(|(token_side, error)| format!("{}={error}", format_token_side_label(*token_side)))
            .collect::<Vec<_>>()
            .join(", ");
        insert_detail(&mut details, "Poly历史抓取错误", errors);
    }
    details
}

fn strategy_readiness_alert(
    summary: &StrategyHealthView,
    uptime_ms: u64,
    signals_seen: usize,
) -> Option<StrategyReadinessAlert> {
    if summary.total_markets == 0
        || signals_seen > 0
        || uptime_ms < STRATEGY_READINESS_ALERT_GRACE_MS
    {
        return None;
    }

    if summary.cex_history_failed_markets > 0 || summary.poly_history_failed_markets > 0 {
        return Some(StrategyReadinessAlert {
            kind: "error",
            summary: format!(
                "策略预热异常：CEX历史失败 {} 个，Poly历史失败 {} 个，Z值就绪 {}/{}",
                summary.cex_history_failed_markets,
                summary.poly_history_failed_markets,
                summary.z_score_ready_markets,
                summary.total_markets
            ),
            details: Some(strategy_health_event_details(summary)),
        });
    }

    if summary.z_score_ready_markets == 0 {
        return Some(StrategyReadinessAlert {
            kind: "risk",
            summary: format!(
                "策略未就绪：0/{} 市场具备 Z 值，预热完成 {}/{}，预热不足 {} 个，门槛 {}",
                summary.total_markets,
                summary.warmup_ready_markets,
                summary.total_markets,
                summary.insufficient_warmup_markets,
                summary.min_warmup_samples
            ),
            details: Some(strategy_health_event_details(summary)),
        });
    }

    None
}

fn apply_strategy_health_to_monitor_state(
    monitor_state: &mut MonitorState,
    warmup_status: &HashMap<Symbol, MarketWarmupStatus>,
    min_warmup_samples: usize,
    warmup_total_markets: usize,
    warmup_completed_markets: usize,
    warmup_failed_markets: usize,
) {
    monitor_state.runtime.strategy_health = build_strategy_health_summary(
        &monitor_state.markets,
        warmup_status,
        min_warmup_samples,
        warmup_total_markets,
        warmup_completed_markets,
        warmup_failed_markets,
    );
}

fn spawn_multi_market_warmup_fetch(
    markets: Vec<MarketConfig>,
    poly_source: PolymarketLiveDataSource,
    cex_sources: HashMap<Exchange, CexLiveSource>,
    warmup_klines: u16,
    warmup_tx: mpsc::Sender<MultiMarketWarmupEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let total_markets = markets.len();
        let concurrency = multi_market_warmup_concurrency(total_markets);
        let started_at_ms = now_millis();
        let _ = warmup_tx
            .send(MultiMarketWarmupEvent::Started {
                total_markets,
                concurrency,
            })
            .await;

        let tasks = markets.into_iter().map(|market| {
            let poly_source = poly_source.clone();
            let cex_sources = cex_sources.clone();
            async move {
                let result = if let Some(source) = cex_sources.get(&market.hedge_exchange) {
                    fetch_market_warmup_data(&market, source, &poly_source, warmup_klines).await
                } else {
                    Ok(MarketWarmupData::default())
                };
                (market, result)
            }
        });

        let mut failed_markets = 0usize;
        let mut stream = futures_util::stream::iter(tasks).buffer_unordered(concurrency);
        while let Some((market, result)) = stream.next().await {
            let event = match result {
                Ok(data) => MultiMarketWarmupEvent::MarketData { market, data },
                Err(err) => {
                    failed_markets += 1;
                    MultiMarketWarmupEvent::MarketFailed {
                        symbol: market.symbol.0.clone(),
                        error: err.to_string(),
                    }
                }
            };
            if warmup_tx.send(event).await.is_err() {
                return;
            }
        }

        let _ = warmup_tx
            .send(MultiMarketWarmupEvent::Finished {
                total_markets,
                failed_markets,
                duration_ms: now_millis().saturating_sub(started_at_ms),
            })
            .await;
    })
}

fn append_recent_event(events: &mut VecDeque<PaperEventLog>, event: PaperEventLog) {
    let mut event = event;
    event.seq = next_recent_event_seq(events);
    insert_recent_event(events, event);
}

fn is_low_priority_recent_event_kind(kind: &str) -> bool {
    matches!(
        kind,
        "market_phase" | "market_data" | "state" | "warning" | "connection" | "command" | "warmup"
    )
}

fn insert_recent_event(events: &mut VecDeque<PaperEventLog>, event: PaperEventLog) {
    if events.len() < MAX_EVENT_LOGS {
        events.push_back(event);
        return;
    }

    let incoming_is_low_priority = is_low_priority_recent_event_kind(&event.kind);
    let eviction_index = if incoming_is_low_priority {
        events
            .iter()
            .position(|existing| is_low_priority_recent_event_kind(&existing.kind))
    } else {
        events
            .iter()
            .position(|existing| is_low_priority_recent_event_kind(&existing.kind))
            .or(Some(0))
    };

    match eviction_index {
        Some(index) => {
            events.remove(index);
            events.push_back(event);
        }
        None => {
            // Drop low-priority noise when the buffer is full of higher-value events.
        }
    }
}

fn drain_command_events(
    command_center: &Arc<Mutex<CommandCenter>>,
    recent_events: &mut VecDeque<PaperEventLog>,
) {
    let drained = command_center
        .lock()
        .map(|mut center| center.drain_events())
        .unwrap_or_default();
    for event in drained {
        append_recent_event(recent_events, event);
    }
}

fn has_open_position_for_symbol(
    grouped_positions: &HashMap<Symbol, GroupedPosition>,
    symbol: &Symbol,
) -> bool {
    grouped_positions
        .get(symbol)
        .map(|grouped| {
            grouped.poly_yes.is_some() || grouped.poly_no.is_some() || grouped.cex.is_some()
        })
        .unwrap_or(false)
}

fn resolved_active_token_side(
    risk: &InMemoryRiskManager,
    trade_book: &TradeBook,
    symbol: &Symbol,
    fallback_token_side: Option<TokenSide>,
) -> Option<TokenSide> {
    let tracker = risk.position_tracker();
    let has_yes_position = !tracker
        .net_symbol_qty(symbol, InstrumentKind::PolyYes)
        .is_zero();
    let has_no_position = !tracker
        .net_symbol_qty(symbol, InstrumentKind::PolyNo)
        .is_zero();

    match (has_yes_position, has_no_position) {
        (true, false) => Some(TokenSide::Yes),
        (false, true) => Some(TokenSide::No),
        (true, true) => trade_book
            .open
            .get(symbol)
            .map(|open| open.token_side)
            .or(fallback_token_side),
        (false, false) => {
            if tracker.symbol_has_open_position(symbol) {
                trade_book
                    .open
                    .get(symbol)
                    .map(|open| open.token_side)
                    .or(fallback_token_side)
            } else {
                None
            }
        }
    }
}

fn sync_engine_position_state_from_runtime(
    engine: &mut SimpleAlphaEngine,
    risk: &InMemoryRiskManager,
    trade_book: &TradeBook,
    symbol: &Symbol,
    fallback_token_side: Option<TokenSide>,
) {
    let token_side = resolved_active_token_side(risk, trade_book, symbol, fallback_token_side);
    engine.sync_position_state(symbol, token_side);
}

#[cfg(test)]
fn open_intent_from_candidate(candidate: &OpenCandidate) -> PlanningIntent {
    runtime_open_intent_from_candidate(candidate)
}

fn close_intent_for_symbol(
    symbol: &Symbol,
    reason: &str,
    correlation_id: &str,
    now_ms: u64,
) -> PlanningIntent {
    runtime_close_intent_for_symbol(symbol, reason, correlation_id, now_ms)
}

fn delta_rebalance_intent_for_symbol(
    symbol: &Symbol,
    correlation_id: &str,
    now_ms: u64,
    residual_snapshot: ResidualSnapshot,
    target_residual_delta_max: f64,
    target_shock_loss_max: f64,
) -> PlanningIntent {
    runtime_delta_rebalance_intent_for_symbol(
        symbol,
        correlation_id,
        now_ms,
        residual_snapshot,
        target_residual_delta_max,
        target_shock_loss_max,
    )
}

fn force_exit_intent_for_symbol(
    symbol: &Symbol,
    reason: &str,
    correlation_id: &str,
    now_ms: u64,
    allow_negative_edge: bool,
) -> PlanningIntent {
    runtime_force_exit_intent_for_symbol(
        symbol,
        reason,
        correlation_id,
        now_ms,
        allow_negative_edge,
    )
}

async fn close_symbol_position(
    symbol: &Symbol,
    command_id: &str,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
    now_ms: u64,
) -> Result<()> {
    let intent = close_intent_for_symbol(
        symbol,
        "manual_command",
        &format!("command-{command_id}"),
        now_ms,
    );
    execute_intent_trigger(
        intent,
        now_ms,
        engine,
        execution,
        risk,
        stats,
        recent_events,
        trade_book,
        audit,
    )
    .await?;
    Ok(())
}

async fn force_exit_symbol_position(
    symbol: &Symbol,
    command_id: &str,
    reason: &str,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
    now_ms: u64,
) -> Result<()> {
    let intent = force_exit_intent_for_symbol(
        symbol,
        reason,
        &format!("command-{command_id}"),
        now_ms,
        true,
    );
    execute_intent_trigger(
        intent,
        now_ms,
        engine,
        execution,
        risk,
        stats,
        recent_events,
        trade_book,
        audit,
    )
    .await?;
    Ok(())
}

fn rebalance_reference_delta(
    trade_book: &TradeBook,
    engine: &SimpleAlphaEngine,
    symbol: &Symbol,
) -> Option<f64> {
    engine
        .basis_snapshot(symbol)
        .map(|snapshot| snapshot.delta)
        .or_else(|| trade_book.open.get(symbol).map(|open| open.delta))
}

fn current_rebalance_snapshot(
    risk: &InMemoryRiskManager,
    trade_book: &TradeBook,
    engine: &SimpleAlphaEngine,
    symbol: &Symbol,
) -> Option<ResidualSnapshot> {
    let tracker = risk.position_tracker();
    if !tracker.symbol_has_open_position(symbol) {
        return None;
    }

    let poly_yes_qty = tracker
        .net_symbol_qty(symbol, InstrumentKind::PolyYes)
        .abs();
    let poly_no_qty = tracker.net_symbol_qty(symbol, InstrumentKind::PolyNo).abs();
    let poly_qty = match (poly_yes_qty.is_zero(), poly_no_qty.is_zero()) {
        (false, true) => poly_yes_qty,
        (true, false) => poly_no_qty,
        (true, true) => Decimal::ZERO,
        (false, false) => poly_yes_qty.max(poly_no_qty),
    };
    if poly_qty.is_zero() {
        return None;
    }

    let hedge_ratio = trade_book
        .open
        .get(symbol)
        .map(|open| open.hedge_ratio)
        .unwrap_or(1.0);
    let delta = rebalance_reference_delta(trade_book, engine, symbol)?;
    let desired_cex_qty = PolyShares(poly_qty).to_cex_base_qty(delta).0
        * Decimal::from_f64(hedge_ratio).unwrap_or(Decimal::ONE);
    let planned_cex_qty = trade_book
        .open
        .get(symbol)
        .and_then(|open| open.last_planned_cex_qty)
        .unwrap_or(desired_cex_qty);
    let target_cex_net_qty = if delta >= 0.0 {
        Decimal::ZERO - desired_cex_qty
    } else {
        desired_cex_qty
    };
    let actual_cex_net_qty = risk.cex_position_qty(symbol);
    let residual_cex_net_qty = target_cex_net_qty - actual_cex_net_qty;
    let preferred_cex_side = if residual_cex_net_qty >= Decimal::ZERO {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let residual_qty = residual_cex_net_qty.abs();
    let residual_delta = residual_qty.to_f64().filter(|value| value.is_finite())?;

    Some(ResidualSnapshot {
        schema_version: PLANNING_SCHEMA_VERSION,
        residual_delta,
        planned_cex_qty: polyalpha_core::CexBaseQty(planned_cex_qty),
        current_poly_yes_shares: PolyShares(poly_yes_qty),
        current_poly_no_shares: PolyShares(poly_no_qty),
        preferred_cex_side,
    })
}

fn rebalance_shock_loss_max(
    observed_map: &HashMap<Symbol, ObservedState>,
    symbol: &Symbol,
    residual_delta: f64,
) -> f64 {
    let reference_price = observed_map
        .get(symbol)
        .and_then(|observed| observed.cex_mid)
        .and_then(|value| value.to_f64())
        .unwrap_or_default();
    if reference_price > 0.0 {
        residual_delta * reference_price * 0.02
    } else {
        residual_delta
    }
}

async fn maybe_execute_delta_rebalance(
    settings: &Settings,
    observed_map: &HashMap<Symbol, ObservedState>,
    symbol: &Symbol,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
    now_ms: u64,
) -> Result<()> {
    let basis = effective_basis_config_for_symbol(settings, symbol);
    let threshold = basis.delta_rebalance_threshold.to_f64().unwrap_or_default();
    if threshold <= 0.0 {
        return Ok(());
    }

    let Some(residual_snapshot) = current_rebalance_snapshot(risk, trade_book, engine, symbol)
    else {
        return Ok(());
    };
    if residual_snapshot.residual_delta <= threshold {
        return Ok(());
    }

    let throttle_ms = basis.delta_rebalance_interval_secs.max(1) * 1_000;
    if stats
        .last_delta_rebalance_at_ms
        .get(symbol)
        .copied()
        .is_some_and(|last| now_ms.saturating_sub(last) < throttle_ms)
    {
        return Ok(());
    }

    let intent = delta_rebalance_intent_for_symbol(
        symbol,
        &format!("corr-delta-rebalance-{}-{now_ms}", symbol.0),
        now_ms,
        residual_snapshot.clone(),
        threshold,
        rebalance_shock_loss_max(observed_map, symbol, residual_snapshot.residual_delta),
    );
    execute_intent_trigger(
        intent,
        now_ms,
        engine,
        execution,
        risk,
        stats,
        recent_events,
        trade_book,
        audit,
    )
    .await?;
    stats
        .last_delta_rebalance_at_ms
        .insert(symbol.clone(), now_ms);
    Ok(())
}

async fn execute_intent_trigger(
    intent: PlanningIntent,
    timestamp_ms: u64,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    let symbol = intent.symbol().clone();
    let fallback_token_side = None;
    let trigger = RuntimeTrigger::Intent {
        intent,
        timestamp_ms,
    };
    let events = match execution
        .process_intent(trigger.intent().cloned().expect("intent trigger"))
        .await
    {
        Ok(events) => events,
        Err(err)
            if track_intent_execution_rejection(
                stats,
                recent_events,
                &trigger,
                &err.to_string(),
                audit,
            )? =>
        {
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &symbol,
                fallback_token_side,
            );
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    apply_execution_events(
        risk,
        stats,
        recent_events,
        trade_book,
        Some(&trigger),
        events,
        audit,
    )
    .await?;
    sync_engine_position_state_from_runtime(engine, risk, trade_book, &symbol, fallback_token_side);
    Ok(())
}

fn finish_command(
    command_center: &Arc<Mutex<CommandCenter>>,
    command_id: &str,
    now_ms: u64,
    status: CommandStatus,
    message: Option<String>,
    error_code: Option<u32>,
) -> Result<()> {
    let mut center = command_center
        .lock()
        .map_err(|_| anyhow!("命令中心锁异常"))?;
    center.finish(command_id, now_ms, status, message, error_code);
    Ok(())
}

fn parse_decimal_field(value: f64, field: &str) -> Result<Decimal> {
    if !value.is_finite() {
        anyhow::bail!("{field} 必须是有限数值");
    }
    Decimal::from_str(&value.to_string()).with_context(|| format!("{field} 不是合法数字"))
}

fn apply_runtime_config_update(
    settings: &mut Settings,
    engine: &mut SimpleAlphaEngine,
    update: &polyalpha_core::ConfigUpdate,
) -> Result<String> {
    let mut changed = Vec::new();
    let mut next_entry = settings
        .strategy
        .basis
        .entry_z_score_threshold
        .to_f64()
        .unwrap_or(0.0)
        .abs()
        .max(0.05);
    let mut next_exit = settings
        .strategy
        .basis
        .exit_z_score_threshold
        .to_f64()
        .unwrap_or(0.0)
        .abs();
    let mut engine_params = EngineParams::default();

    if let Some(entry_z) = update.entry_z {
        next_entry = entry_z.abs().max(0.05);
        if next_exit > next_entry {
            next_exit = next_entry * 0.5;
        }
        engine_params.basis_entry_zscore = Some(next_entry);
        changed.push(format!("入场Z={next_entry:.3}"));
    }

    if let Some(exit_z) = update.exit_z {
        next_exit = exit_z.abs().min(next_entry.max(0.05));
        engine_params.basis_exit_zscore = Some(next_exit);
        changed.push(format!("出场Z={next_exit:.3}"));
    } else if update.entry_z.is_some() {
        engine_params.basis_exit_zscore = Some(next_exit);
    }

    if let Some(rolling_window) = update.rolling_window {
        if rolling_window < 120 {
            anyhow::bail!("滚动窗口至少需要 120 秒");
        }
        settings.strategy.basis.rolling_window_secs = rolling_window;
        engine_params.rolling_window_secs = Some(rolling_window);
        changed.push(format!("滚动窗口={}s", rolling_window));
    }

    if let Some(position_notional_usd) = update.position_notional_usd {
        if position_notional_usd <= 0.0 {
            anyhow::bail!("单次仓位必须大于 0");
        }
        let notional = UsdNotional(parse_decimal_field(
            position_notional_usd,
            "position_notional_usd",
        )?);
        settings.strategy.basis.max_position_usd = notional;
        engine_params.max_position_usd = Some(notional);
        changed.push(format!("单次仓位=${position_notional_usd:.2}"));
    }

    if let Some(band_policy_raw) = update.band_policy.as_deref() {
        let band_policy = polyalpha_core::BandPolicyMode::from_str(band_policy_raw)
            .map_err(|err| anyhow!("{err}"))?;
        settings.strategy.basis.band_policy = band_policy;
        changed.push(format!("价格带策略={band_policy_raw}"));
    }

    if let Some(min_poly_price) = update.min_poly_price {
        if !(0.0..=1.0).contains(&min_poly_price) {
            anyhow::bail!("最小 Polymarket 价格必须位于 0 到 1 之间");
        }
        settings.strategy.basis.min_poly_price =
            Some(parse_decimal_field(min_poly_price, "min_poly_price")?);
        changed.push(format!("最小Polymarket价格={min_poly_price:.4}"));
    }

    if let Some(max_poly_price) = update.max_poly_price {
        if !(0.0..=1.0).contains(&max_poly_price) {
            anyhow::bail!("最大 Polymarket 价格必须位于 0 到 1 之间");
        }
        settings.strategy.basis.max_poly_price =
            Some(parse_decimal_field(max_poly_price, "max_poly_price")?);
        changed.push(format!("最大Polymarket价格={max_poly_price:.4}"));
    }

    if let (Some(min_poly_price), Some(max_poly_price)) = (
        settings.strategy.basis.min_poly_price,
        settings.strategy.basis.max_poly_price,
    ) {
        if min_poly_price > max_poly_price {
            anyhow::bail!("最小 Polymarket 价格不能大于最大 Polymarket 价格");
        }
    }

    if changed.is_empty() {
        anyhow::bail!("未提供可更新的配置项");
    }

    settings.strategy.basis.entry_z_score_threshold = parse_decimal_field(next_entry, "entry_z")?;
    settings.strategy.basis.exit_z_score_threshold = parse_decimal_field(next_exit, "exit_z")?;
    engine.update_params(engine_params);

    Ok(format!("已更新参数：{}", changed.join(", ")))
}

async fn process_pending_command_single(
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    pre_emergency_paused: &Arc<AtomicBool>,
    settings: &mut Settings,
    market: &MarketConfig,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
    now_ms: u64,
) -> Result<bool> {
    let mut processed_any = false;
    loop {
        let command = {
            let mut center = command_center
                .lock()
                .map_err(|_| anyhow!("命令中心锁异常"))?;
            center.start_next(now_ms)
        };
        let Some(command) = command else {
            break;
        };
        processed_any = true;

        let outcome = match command.kind.clone() {
            CommandKind::Pause => {
                paused.store(true, Ordering::SeqCst);
                Ok((CommandStatus::Success, "交易已暂停".to_owned(), None))
            }
            CommandKind::Resume => {
                if emergency.load(Ordering::SeqCst) {
                    Err((
                        CommandStatus::Failed,
                        "处于紧急停止状态，请先解除紧急停止".to_owned(),
                        None,
                    ))
                } else {
                    paused.store(false, Ordering::SeqCst);
                    Ok((CommandStatus::Success, "交易已恢复".to_owned(), None))
                }
            }
            CommandKind::EmergencyStop => {
                if !emergency.swap(true, Ordering::SeqCst) {
                    pre_emergency_paused.store(paused.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                paused.store(true, Ordering::SeqCst);
                let grouped_positions = group_positions_by_symbol(risk, now_ms);
                if has_open_position_for_symbol(&grouped_positions, &market.symbol) {
                    match force_exit_symbol_position(
                        &market.symbol,
                        &command.command_id,
                        "emergency_stop",
                        engine,
                        execution,
                        risk,
                        stats,
                        recent_events,
                        trade_book,
                        audit,
                        now_ms,
                    )
                    .await
                    {
                        Ok(()) => Ok((
                            CommandStatus::Success,
                            format!("紧急停止已激活，并已提交 {} 强制退出", market.symbol.0),
                            None,
                        )),
                        Err(err) => Err((CommandStatus::Failed, err.to_string(), Some(2006))),
                    }
                } else {
                    Ok((CommandStatus::Success, "紧急停止已激活".to_owned(), None))
                }
            }
            CommandKind::ClearEmergency => {
                emergency.store(false, Ordering::SeqCst);
                paused.store(
                    pre_emergency_paused.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                Ok((CommandStatus::Success, "紧急停止已清除".to_owned(), None))
            }
            CommandKind::ClosePosition {
                market: target_market,
            } => {
                if target_market != market.symbol.0 {
                    Err((
                        CommandStatus::Failed,
                        format!("未知市场：{target_market}"),
                        Some(2001),
                    ))
                } else {
                    let grouped_positions = group_positions_by_symbol(risk, now_ms);
                    if !has_open_position_for_symbol(&grouped_positions, &market.symbol) {
                        Err((
                            CommandStatus::Failed,
                            "当前市场没有可平仓持仓".to_owned(),
                            Some(2002),
                        ))
                    } else {
                        match close_symbol_position(
                            &market.symbol,
                            &command.command_id,
                            engine,
                            execution,
                            risk,
                            stats,
                            recent_events,
                            trade_book,
                            audit,
                            now_ms,
                        )
                        .await
                        {
                            Ok(()) => Ok((
                                CommandStatus::Success,
                                format!("已提交 {} 平仓", market.symbol.0),
                                None,
                            )),
                            Err(err) => Err((CommandStatus::Failed, err.to_string(), Some(2003))),
                        }
                    }
                }
            }
            CommandKind::CloseAllPositions => {
                let grouped_positions = group_positions_by_symbol(risk, now_ms);
                if !has_open_position_for_symbol(&grouped_positions, &market.symbol) {
                    Ok((
                        CommandStatus::Success,
                        "当前无持仓，无需平仓".to_owned(),
                        None,
                    ))
                } else {
                    match close_symbol_position(
                        &market.symbol,
                        &command.command_id,
                        engine,
                        execution,
                        risk,
                        stats,
                        recent_events,
                        trade_book,
                        audit,
                        now_ms,
                    )
                    .await
                    {
                        Ok(()) => Ok((
                            CommandStatus::Success,
                            "已平掉当前全部持仓".to_owned(),
                            None,
                        )),
                        Err(err) => Err((CommandStatus::Failed, err.to_string(), Some(2004))),
                    }
                }
            }
            CommandKind::UpdateConfig { config } => {
                apply_runtime_config_update(settings, engine, &config)
                    .map(|message| (CommandStatus::Success, message, None))
                    .map_err(|err| (CommandStatus::Failed, err.to_string(), Some(2005)))
            }
            CommandKind::CancelCommand { .. } => Ok((
                CommandStatus::Success,
                command
                    .message
                    .unwrap_or_else(|| "取消命令已处理".to_owned()),
                command.error_code,
            )),
        };

        let (status, message, error_code) = match outcome {
            Ok(value) => value,
            Err(value) => value,
        };
        finish_command(
            command_center,
            &command.command_id,
            now_ms,
            status,
            Some(message),
            error_code,
        )?;
    }

    Ok(processed_any)
}

fn resolve_market_symbol(markets: &[MarketConfig], requested: &str) -> Option<Symbol> {
    markets
        .iter()
        .find(|market| market.symbol.0 == requested || market.cex_symbol == requested)
        .map(|market| market.symbol.clone())
}

async fn process_pending_command_multi(
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    pre_emergency_paused: &Arc<AtomicBool>,
    settings: &mut Settings,
    markets: &[MarketConfig],
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    audit: &mut Option<PaperAudit>,
    now_ms: u64,
) -> Result<bool> {
    let mut processed_any = false;
    loop {
        let command = {
            let mut center = command_center
                .lock()
                .map_err(|_| anyhow!("命令中心锁异常"))?;
            center.start_next(now_ms)
        };
        let Some(command) = command else {
            break;
        };
        processed_any = true;

        let outcome = match command.kind.clone() {
            CommandKind::Pause => {
                paused.store(true, Ordering::SeqCst);
                Ok((CommandStatus::Success, "交易已暂停".to_owned(), None))
            }
            CommandKind::Resume => {
                if emergency.load(Ordering::SeqCst) {
                    Err((
                        CommandStatus::Failed,
                        "处于紧急停止状态，请先解除紧急停止".to_owned(),
                        None,
                    ))
                } else {
                    paused.store(false, Ordering::SeqCst);
                    Ok((CommandStatus::Success, "交易已恢复".to_owned(), None))
                }
            }
            CommandKind::EmergencyStop => {
                if !emergency.swap(true, Ordering::SeqCst) {
                    pre_emergency_paused.store(paused.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                paused.store(true, Ordering::SeqCst);
                let grouped_positions = group_positions_by_symbol(risk, now_ms);
                let symbols_to_force_exit: Vec<Symbol> = markets
                    .iter()
                    .filter(|market| {
                        has_open_position_for_symbol(&grouped_positions, &market.symbol)
                    })
                    .map(|market| market.symbol.clone())
                    .collect();
                let mut force_exit_error = None;
                for symbol in &symbols_to_force_exit {
                    if let Err(err) = force_exit_symbol_position(
                        symbol,
                        &command.command_id,
                        "emergency_stop",
                        engine,
                        execution,
                        risk,
                        stats,
                        recent_events,
                        trade_book,
                        audit,
                        now_ms,
                    )
                    .await
                    {
                        force_exit_error = Some(err);
                        break;
                    }
                }
                if let Some(err) = force_exit_error {
                    Err((CommandStatus::Failed, err.to_string(), Some(2106)))
                } else if symbols_to_force_exit.is_empty() {
                    Ok((CommandStatus::Success, "紧急停止已激活".to_owned(), None))
                } else {
                    Ok((
                        CommandStatus::Success,
                        format!(
                            "紧急停止已激活，并已提交 {} 个市场强制退出",
                            symbols_to_force_exit.len()
                        ),
                        None,
                    ))
                }
            }
            CommandKind::ClearEmergency => {
                emergency.store(false, Ordering::SeqCst);
                paused.store(
                    pre_emergency_paused.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                Ok((CommandStatus::Success, "紧急停止已清除".to_owned(), None))
            }
            CommandKind::ClosePosition { market } => {
                if let Some(symbol) = resolve_market_symbol(markets, &market) {
                    let grouped_positions = group_positions_by_symbol(risk, now_ms);
                    if !has_open_position_for_symbol(&grouped_positions, &symbol) {
                        Err((
                            CommandStatus::Failed,
                            format!("{} 没有可平仓持仓", symbol.0),
                            Some(2102),
                        ))
                    } else {
                        match close_symbol_position(
                            &symbol,
                            &command.command_id,
                            engine,
                            execution,
                            risk,
                            stats,
                            recent_events,
                            trade_book,
                            audit,
                            now_ms,
                        )
                        .await
                        {
                            Ok(()) => Ok((
                                CommandStatus::Success,
                                format!("已提交 {} 平仓", symbol.0),
                                None,
                            )),
                            Err(err) => Err((CommandStatus::Failed, err.to_string(), Some(2103))),
                        }
                    }
                } else {
                    Err((
                        CommandStatus::Failed,
                        format!("未知市场：{market}"),
                        Some(2101),
                    ))
                }
            }
            CommandKind::CloseAllPositions => {
                let grouped_positions = group_positions_by_symbol(risk, now_ms);
                let symbols = grouped_positions
                    .keys()
                    .filter(|symbol| has_open_position_for_symbol(&grouped_positions, symbol))
                    .cloned()
                    .collect::<Vec<_>>();
                if symbols.is_empty() {
                    Ok((
                        CommandStatus::Success,
                        "当前无持仓，无需平仓".to_owned(),
                        None,
                    ))
                } else {
                    let mut failed = Vec::new();
                    for symbol in &symbols {
                        if let Err(err) = close_symbol_position(
                            symbol,
                            &command.command_id,
                            engine,
                            execution,
                            risk,
                            stats,
                            recent_events,
                            trade_book,
                            audit,
                            now_ms,
                        )
                        .await
                        {
                            failed.push(format!("{}: {}", symbol.0, err));
                        }
                    }

                    if failed.is_empty() {
                        Ok((
                            CommandStatus::Success,
                            format!("已提交 {} 个市场平仓", symbols.len()),
                            None,
                        ))
                    } else if failed.len() == symbols.len() {
                        Err((CommandStatus::Failed, failed.join("; "), Some(2104)))
                    } else {
                        Ok((
                            CommandStatus::PartialSuccess,
                            format!("部分市场平仓失败: {}", failed.join("; ")),
                            Some(2104),
                        ))
                    }
                }
            }
            CommandKind::UpdateConfig { config } => {
                apply_runtime_config_update(settings, engine, &config)
                    .map(|message| (CommandStatus::Success, message, None))
                    .map_err(|err| (CommandStatus::Failed, err.to_string(), Some(2105)))
            }
            CommandKind::CancelCommand { .. } => Ok((
                CommandStatus::Success,
                command
                    .message
                    .unwrap_or_else(|| "取消命令已处理".to_owned()),
                command.error_code,
            )),
        };

        let (status, message, error_code) = match outcome {
            Ok(value) => value,
            Err(value) => value,
        };
        finish_command(
            command_center,
            &command.command_id,
            now_ms,
            status,
            Some(message),
            error_code,
        )?;
    }

    Ok(processed_any)
}

fn candidate_price_for_filter(candidate: &OpenCandidate, observed: &ObservedState) -> Option<f64> {
    match candidate.token_side {
        TokenSide::Yes => observed.poly_yes_mid.and_then(|value| value.to_f64()),
        TokenSide::No => observed.poly_no_mid.and_then(|value| value.to_f64()),
    }
}

fn market_open_semantics_guard_reason(
    market: &MarketConfig,
    scope: &StrategyMarketScopeConfig,
    now_ms: u64,
) -> Option<&'static str> {
    let _ = now_ms;
    main_strategy_rejection_reason(
        scope,
        market.market_question.as_deref(),
        &market.symbol.0,
        market.market_rule.as_ref(),
    )
}

fn market_runtime_expired_settlement_reason(
    market: &MarketConfig,
    now_timestamp_secs: u64,
) -> Option<&'static str> {
    (market.settlement_timestamp <= now_timestamp_secs).then_some("expired_settlement_market")
}

fn filter_runtime_markets_for_expired_settlement(
    settings: &mut Settings,
    now_timestamp_secs: u64,
) -> Vec<Symbol> {
    let mut skipped = Vec::new();
    settings.markets.retain(|market| {
        if market_runtime_expired_settlement_reason(market, now_timestamp_secs).is_some() {
            skipped.push(market.symbol.clone());
            false
        } else {
            true
        }
    });
    skipped
}

fn filter_runtime_markets_for_main_strategy(
    settings: &mut Settings,
) -> Vec<(Symbol, &'static str)> {
    let mut skipped = Vec::new();
    settings.markets.retain(|market| {
        if let Some(reason) = main_strategy_rejection_reason(
            &settings.strategy.market_scope,
            market.market_question.as_deref(),
            &market.symbol.0,
            market.market_rule.as_ref(),
        ) {
            skipped.push((market.symbol.clone(), reason));
            false
        } else {
            true
        }
    });
    skipped
}

fn log_runtime_expired_settlement_filter(skipped: &[Symbol]) {
    if skipped.is_empty() {
        return;
    }

    println!("已从运行集移除 {} 个已过结算时间的市场：", skipped.len());
    for symbol in skipped.iter().take(8) {
        println!("  {}：市场结算时间已过", symbol.0);
    }
    if skipped.len() > 8 {
        println!("  其余 {} 个市场已省略", skipped.len() - 8);
    }
    println!();
}

fn log_runtime_market_scope_filter(skipped: &[(Symbol, &'static str)]) {
    if skipped.is_empty() {
        return;
    }

    println!("已从运行集移除 {} 个未纳入主策略的市场：", skipped.len());
    for (symbol, reason) in skipped.iter().take(8) {
        println!("  {}：{}", symbol.0, strategy_rejection_label_zh(reason));
    }
    if skipped.len() > 8 {
        println!("  其余 {} 个市场已省略", skipped.len() - 8);
    }
    println!();
}

fn candidate_price_filter_rejection_reason(
    candidate: &OpenCandidate,
    observed: &ObservedState,
    settings: &Settings,
) -> Option<&'static str> {
    let market = settings
        .markets
        .iter()
        .find(|market| market.symbol == candidate.symbol);
    if let Some(reason) = market.and_then(|market| {
        market_open_semantics_guard_reason(
            market,
            &settings.strategy.market_scope,
            candidate.timestamp_ms,
        )
    }) {
        return Some(reason);
    }
    let basis = market
        .map(|market| effective_basis_config_for_market(settings, market))
        .unwrap_or_else(|| global_basis_config(settings));
    if !basis.band_policy.uses_configured_band() {
        return None;
    }
    let min_price = basis.min_poly_price.and_then(|value| value.to_f64());
    let max_price = basis.max_poly_price.and_then(|value| value.to_f64());
    if min_price.is_none() && max_price.is_none() {
        return None;
    }

    let Some(price) = candidate_price_for_filter(candidate, observed) else {
        return Some("missing_poly_quote");
    };
    if min_price.is_some_and(|min| price < min) {
        return Some("below_min_poly_price");
    }
    if max_price.is_some_and(|max| price >= max) {
        return Some("above_or_equal_max_poly_price");
    }
    None
}

#[cfg(test)]
fn candidate_passes_price_filter(
    candidate: &OpenCandidate,
    observed: &ObservedState,
    settings: &Settings,
) -> bool {
    candidate_price_filter_rejection_reason(candidate, observed, settings).is_none()
}

fn candidate_price_filter_rejection_reason_multi(
    candidate: &OpenCandidate,
    observed_map: &HashMap<Symbol, ObservedState>,
    settings: &Settings,
) -> Option<&'static str> {
    match observed_map.get(&candidate.symbol) {
        Some(observed) => candidate_price_filter_rejection_reason(candidate, observed, settings),
        None if settings.strategy.basis.band_policy.uses_configured_band() => {
            Some("missing_poly_quote")
        }
        None => None,
    }
}

#[cfg(test)]
fn candidate_passes_price_filter_multi(
    candidate: &OpenCandidate,
    observed_map: &HashMap<Symbol, ObservedState>,
    settings: &Settings,
) -> bool {
    candidate_price_filter_rejection_reason_multi(candidate, observed_map, settings).is_none()
}

fn resolve_live_execution_mode(
    executor_mode: LiveExecutorMode,
    confirm_live: bool,
) -> Result<RuntimeExecutionMode> {
    match executor_mode {
        LiveExecutorMode::Mock => Ok(RuntimeExecutionMode::LiveMock),
        LiveExecutorMode::Live if !confirm_live => {
            anyhow::bail!("armed live execution requires --confirm-live")
        }
        LiveExecutorMode::Live => Ok(RuntimeExecutionMode::Live),
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
    warmup_klines: u16,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    run_runtime_single(
        env,
        settings,
        market_index,
        poll_interval_ms,
        print_every,
        max_ticks,
        depth,
        include_funding,
        json,
        warmup_klines,
        RuntimeExecutionMode::Paper,
        polyalpha_core::TradingMode::Paper,
    )
    .await
}

pub async fn run_live(
    env: &str,
    market_index: usize,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    executor_mode: LiveExecutorMode,
    confirm_live: bool,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    run_runtime_single(
        env,
        settings,
        market_index,
        poll_interval_ms,
        print_every,
        max_ticks,
        depth,
        include_funding,
        json,
        warmup_klines,
        resolve_live_execution_mode(executor_mode, confirm_live)?,
        polyalpha_core::TradingMode::Live,
    )
    .await
}

async fn run_runtime_single(
    env: &str,
    mut settings: Settings,
    market_index: usize,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    execution_mode: RuntimeExecutionMode,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<()> {
    let requested_market = select_market(&settings, market_index)?.clone();
    let now_timestamp_secs = now_millis() / 1000;
    if market_runtime_expired_settlement_reason(&requested_market, now_timestamp_secs).is_some() {
        anyhow::bail!("{} 已过结算时间，禁止运行", requested_market.symbol.0);
    }
    if let Some(reason) = main_strategy_rejection_reason(
        &settings.strategy.market_scope,
        requested_market.market_question.as_deref(),
        &requested_market.symbol.0,
        requested_market.market_rule.as_ref(),
    ) {
        anyhow::bail!(
            "{} 未纳入当前主策略：{}",
            requested_market.symbol.0,
            strategy_rejection_label_zh(reason)
        );
    }
    let requested_symbol = requested_market.symbol.clone();
    let expired_skipped =
        filter_runtime_markets_for_expired_settlement(&mut settings, now_timestamp_secs);
    log_runtime_expired_settlement_filter(&expired_skipped);
    let skipped = filter_runtime_markets_for_main_strategy(&mut settings);
    log_runtime_market_scope_filter(&skipped);
    let filtered_market_index = settings
        .markets
        .iter()
        .position(|market| market.symbol == requested_symbol)
        .ok_or_else(|| anyhow!("选中的市场已被运行集过滤：{}", requested_symbol.0))?;

    if matches!(settings.strategy.market_data.mode, MarketDataMode::Ws) {
        return run_paper_ws_mode(
            env,
            settings,
            filtered_market_index,
            poll_interval_ms,
            print_every,
            max_ticks,
            depth,
            include_funding,
            json,
            warmup_klines,
            execution_mode,
            trading_mode,
        )
        .await;
    }
    let market = select_market(&settings, filtered_market_index)?;
    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());

    // Create socket server for monitor
    let socket_path = monitor_socket_path(&settings);
    let current_state: Arc<std::sync::RwLock<Option<MonitorState>>> =
        Arc::new(std::sync::RwLock::new(None));
    let paused = Arc::new(AtomicBool::new(false));
    let emergency = Arc::new(AtomicBool::new(false));
    let pre_emergency_paused = Arc::new(AtomicBool::new(false));
    let command_center = Arc::new(Mutex::new(CommandCenter::default()));

    let socket_server = MonitorSocketServer::new_with_current_state(
        socket_path.clone(),
        Arc::clone(&current_state),
    );
    let state_broadcaster = socket_server.state_broadcaster();
    let event_broadcaster = socket_server.event_broadcaster();
    let command_center_clone = Arc::clone(&command_center);

    // Spawn socket server task
    tokio::spawn(async move {
        let handler = move |command_id: String, kind: CommandKind| {
            let now_ms = now_millis();
            let mut center = command_center_clone
                .lock()
                .map_err(|_| "命令中心锁异常".to_owned())?;
            let command = match kind {
                CommandKind::CancelCommand { command_id: target } => {
                    center.cancel(command_id, target, now_ms)
                }
                other => center.enqueue(command_id, other, now_ms),
            };
            Ok(CommandCenter::ack_message(&command))
        };
        if let Err(e) = socket_server.run(Box::new(handler)).await {
            tracing::error!("监控 Socket 服务异常: {}", e);
        }
    });

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
    let market_overrides = build_market_overrides(&settings, &settings.markets);

    // Log per-market parameters
    let entry_z = market_overrides
        .get(&market.symbol)
        .and_then(|o| o.entry_z)
        .unwrap_or(engine_config.entry_z);
    println!(
        "参数：{} ({}) 入场Z = {}（默认：{}）",
        market.symbol.0, market.cex_symbol, entry_z, engine_config.entry_z
    );

    let mut engine =
        SimpleAlphaEngine::with_markets(engine_config.clone(), settings.markets.clone());
    engine.set_market_overrides(market_overrides);
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor_start_ms = now_millis();
    let (orderbook_provider, executor, mut execution) = build_paper_execution_stack(
        &settings,
        &registry,
        executor_start_ms,
        execution_mode,
        Some(&market),
    )
    .await?;
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed = ObservedState::default();
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut last_event_seq_sent = 0_u64;
    let mut snapshots = Vec::new();
    let mut trade_book = TradeBook::default();
    let mut printed_placeholder_warning = false;
    let start_time_ms = executor_start_ms;
    let mut audit = if settings.audit.enabled {
        let audit = PaperAudit::start(
            &settings,
            env,
            std::slice::from_ref(&market),
            executor_start_ms,
            trading_mode,
        )?;
        println!(
            "审计会话已启动：{} -> {}",
            audit.session_id(),
            audit.session_dir().display()
        );
        Some(audit)
    } else {
        None
    };

    if warmup_klines > 0 {
        if let Err(err) = warmup_market_engine(
            &market,
            &mut engine,
            &cex_source,
            &poly_source,
            warmup_klines,
        )
        .await
        {
            push_event_simple(
                &mut recent_events,
                "warmup",
                format!("{} 预热失败：{}", market.symbol.0, err),
            );
        }
    }

    let loop_result: Result<()> = async {
        loop {
            stats.ticks_processed += 1;
            let tick_index = stats.ticks_processed;
            let now_ms = now_millis();
            let now_secs = now_ms / 1000;

            drain_command_events(&command_center, &mut recent_events);
            let _ = process_pending_command_single(
                &command_center,
                &paused,
                &emergency,
                &pre_emergency_paused,
                &mut settings,
                &market,
                &mut engine,
                &mut execution,
                &mut risk,
                &mut stats,
                &mut recent_events,
                &mut trade_book,
                &mut audit,
                now_ms,
            )
            .await?;
            drain_command_events(&command_center, &mut recent_events);

            if next_market_lifecycle_phase(
                Some(&observed),
                &market,
                now_secs,
                &settings.strategy.settlement,
            )
            .is_some()
            {
                if let Err(err) = manager.publish_market_lifecycle(
                    &market.symbol,
                    now_secs,
                    now_ms,
                    &settings.strategy.settlement,
                ) {
                    stats.poll_errors += 1;
                    push_event_simple(
                        &mut recent_events,
                        "poll_error",
                        format!("市场生命周期发布失败：{err}"),
                    );
                }
            } else {
                observed.market_phase = Some(current_market_lifecycle_phase(
                    &market,
                    now_secs,
                    &settings.strategy.settlement,
                ));
            }

            if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
                || looks_like_placeholder_id(&market.poly_ids.no_token_id)
            {
                if !printed_placeholder_warning {
                    printed_placeholder_warning = true;
                    push_event_simple(
                        &mut recent_events,
                        "warning",
                        "Polymarket token_id 仍是占位值，已跳过双边拉取；请先执行 markets discover-btc 生成 live overlay"
                            .to_owned(),
                    );
                }
            } else {
                for token_side in [TokenSide::Yes, TokenSide::No] {
                    if let Err(err) = poly_source
                        .fetch_and_publish_orderbook_by_symbol(&market.symbol, token_side)
                        .await
                    {
                        let err_str = err.to_string();
                        if err_str.contains("404") || err_str.contains("No orderbook exists") {
                            if token_side == TokenSide::Yes {
                                push_event_simple(
                                    &mut recent_events,
                                    "market_closed",
                                    format!("{} 疑似已关闭或已结算", market.symbol.0),
                                );
                            }
                        } else {
                            stats.poll_errors += 1;
                            push_event_simple(
                                &mut recent_events,
                                "poll_error",
                                format!(
                                    "Polymarket {} 盘口轮询失败：{err}",
                                    format_token_side_label(token_side)
                                ),
                            );
                        }
                    }
                }
            }

            if let Err(err) = cex_source
                .poll_symbol(&market.symbol, depth, include_funding)
                .await
            {
                stats.poll_errors += 1;
                push_event_simple(
                    &mut recent_events,
                    "poll_error",
                    format!("交易所行情轮询失败：{err}"),
                );
            }

            let connection_checked_at_ms = now_millis();
            update_connection_state(
                &mut observed,
                Exchange::Polymarket,
                poly_source.connection_status(),
                connection_checked_at_ms,
            );
            update_connection_state(
                &mut observed,
                market.hedge_exchange,
                cex_source.connection_status(),
                connection_checked_at_ms,
            );

            loop {
                match market_data_rx.try_recv() {
                    Ok(event) => {
                        handle_single_market_event(
                            event,
                            &market,
                            &settings,
                            &registry,
                            &orderbook_provider,
                            &mut observed,
                            &mut stats,
                            &mut recent_events,
                            &mut engine,
                            &mut execution,
                            &mut risk,
                            &mut trade_book,
                            &paused,
                            &emergency,
                            &mut audit,
                        )
                        .await?;
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
                        println!(
                            "最近事件=[{}] {}",
                            display_event_kind_text(&last_event.kind),
                            last_event.summary
                        );
                    }
                }
            }
            snapshots.push(snapshot.clone());

            let monitor_events = build_monitor_events(&recent_events);
            let monitor_state = build_monitor_state(
                &settings,
                &market,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_ms,
                &monitor_events,
                trading_mode,
            );

            if let Ok(mut guard) = current_state.write() {
                *guard = Some(monitor_state.clone());
            }
            broadcast_monitor_events_since(
                &recent_events,
                &mut last_event_seq_sent,
                &event_broadcaster,
            );
            if let Some(audit) = audit.as_mut() {
                audit.maybe_record_runtime_state(&stats, &monitor_state, tick_index, now_ms)?;
            }
            let _ = state_broadcaster.send(monitor_state);

            if max_ticks > 0 && tick_index >= max_ticks {
                break;
            }
            if poll_interval_ms > 0 {
                sleep(Duration::from_millis(poll_interval_ms)).await;
            }
        }
        Ok(())
    }
    .await;

    let final_monitor_state = current_state
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| {
            let monitor_events = build_monitor_events(&recent_events);
            build_monitor_state(
                &settings,
                &market,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_millis(),
                &monitor_events,
                trading_mode,
            )
        });
    if let Ok(mut guard) = current_state.write() {
        *guard = Some(final_monitor_state.clone());
    }
    let runtime_result: Result<()> = async {
        loop_result?;
        let final_snapshot = snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("纸面盘会话未生成任何快照"))?;
        let open_orders = executor
            .open_order_snapshots()?
            .into_iter()
            .map(order_view_from_snapshot)
            .collect();
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode,
            env: env.to_owned(),
            market_index,
            market: market.symbol.0.clone(),
            hedge_exchange: format!("{:?}", market.hedge_exchange),
            market_data_mode: Some("poll".to_owned()),
            poll_interval_ms,
            depth,
            include_funding,
            ticks_processed: stats.ticks_processed,
            final_snapshot,
            open_orders,
            recent_events: recent_events.into_iter().collect(),
            snapshots,
        };
        let artifact_path = artifact_path(&settings, env, trading_mode)?;
        persist_artifact(&artifact_path, &artifact)?;
        println!(
            "{}结果已写入：{}",
            artifact_runtime_label(trading_mode),
            artifact_path.display()
        );
        Ok(())
    }
    .await;
    finish_runtime_with_audit(
        runtime_result,
        &mut audit,
        &stats,
        &final_monitor_state,
        stats.ticks_processed,
        now_millis(),
    )?;
    if let Some(audit) = audit.as_ref() {
        println!("审计会话已完成：{}", audit.session_dir().display());
    }

    Ok(())
}

pub fn inspect_paper(env: &str, format: PaperInspectFormat) -> Result<()> {
    inspect_runtime_artifact(env, polyalpha_core::TradingMode::Paper, format)
}

pub fn inspect_live(env: &str, format: PaperInspectFormat) -> Result<()> {
    inspect_runtime_artifact(env, polyalpha_core::TradingMode::Live, format)
}

fn inspect_runtime_artifact(
    env: &str,
    trading_mode: polyalpha_core::TradingMode,
    format: PaperInspectFormat,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    let artifact_path = resolve_artifact_for_read(&settings, env, trading_mode)?;
    let runtime_label = artifact_runtime_label(trading_mode);
    let raw = fs::read_to_string(&artifact_path)
        .with_context(|| format!("读取{runtime_label}结果 `{}` 失败", artifact_path.display()))?;
    let artifact: PaperArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("解析{runtime_label}结果 `{}` 失败", artifact_path.display()))?;

    match format {
        PaperInspectFormat::Table => print_artifact_table(&artifact, &artifact_path),
        PaperInspectFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&artifact)?);
        }
    }

    Ok(())
}

pub async fn run_paper_multi(
    env: &str,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    run_runtime_multi(
        env,
        settings,
        poll_interval_ms,
        print_every,
        max_ticks,
        depth,
        include_funding,
        json,
        warmup_klines,
        RuntimeExecutionMode::Paper,
        polyalpha_core::TradingMode::Paper,
    )
    .await
}

pub async fn run_live_multi(
    env: &str,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    executor_mode: LiveExecutorMode,
    confirm_live: bool,
) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    run_runtime_multi(
        env,
        settings,
        poll_interval_ms,
        print_every,
        max_ticks,
        depth,
        include_funding,
        json,
        warmup_klines,
        resolve_live_execution_mode(executor_mode, confirm_live)?,
        polyalpha_core::TradingMode::Live,
    )
    .await
}

async fn run_runtime_multi(
    env: &str,
    mut settings: Settings,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    execution_mode: RuntimeExecutionMode,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<()> {
    let now_timestamp_secs = now_millis() / 1000;
    let expired_skipped =
        filter_runtime_markets_for_expired_settlement(&mut settings, now_timestamp_secs);
    log_runtime_expired_settlement_filter(&expired_skipped);
    if settings.markets.is_empty() {
        anyhow::bail!("配置中的市场均已过结算时间");
    }

    let skipped = filter_runtime_markets_for_main_strategy(&mut settings);
    log_runtime_market_scope_filter(&skipped);
    if settings.markets.is_empty() {
        anyhow::bail!("配置中的市场均未纳入当前主策略");
    }

    if matches!(settings.strategy.market_data.mode, MarketDataMode::Ws) {
        return run_paper_multi_ws_mode(
            env,
            settings,
            poll_interval_ms,
            print_every,
            max_ticks,
            depth,
            include_funding,
            json,
            warmup_klines,
            execution_mode,
            trading_mode,
        )
        .await;
    }
    let markets = settings.markets.clone();

    if markets.is_empty() {
        anyhow::bail!("未配置任何市场");
    }

    let registry = SymbolRegistry::new(markets.clone());
    let symbols: Vec<Symbol> = markets.iter().map(|m| m.symbol.clone()).collect();
    let channels = create_channels(&symbols);
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());

    // Create socket server for monitor
    let socket_path = monitor_socket_path(&settings);
    let current_state: Arc<std::sync::RwLock<Option<MonitorState>>> =
        Arc::new(std::sync::RwLock::new(None));
    let paused = Arc::new(AtomicBool::new(false));
    let emergency = Arc::new(AtomicBool::new(false));
    let pre_emergency_paused = Arc::new(AtomicBool::new(false));
    let command_center = Arc::new(Mutex::new(CommandCenter::default()));

    let socket_server = MonitorSocketServer::new_with_current_state(
        socket_path.clone(),
        Arc::clone(&current_state),
    );
    let monitor_client_count = socket_server.client_count_handle();
    let state_broadcaster = socket_server.state_broadcaster();
    let event_broadcaster = socket_server.event_broadcaster();
    let command_center_clone = Arc::clone(&command_center);

    // Spawn socket server task
    tokio::spawn(async move {
        let handler = move |command_id: String, kind: CommandKind| {
            let now_ms = now_millis();
            let mut center = command_center_clone
                .lock()
                .map_err(|_| "命令中心锁异常".to_owned())?;
            let command = match kind {
                CommandKind::CancelCommand { command_id: target } => {
                    center.cancel(command_id, target, now_ms)
                }
                other => center.enqueue(command_id, other, now_ms),
            };
            Ok(CommandCenter::ack_message(&command))
        };
        if let Err(e) = socket_server.run(Box::new(handler)).await {
            tracing::error!("监控 Socket 服务异常: {}", e);
        }
    });

    // Create polymarket source (shared across all markets)
    let mut poly_source = PolymarketLiveDataSource::new(
        manager.clone(),
        settings.polymarket.clob_api_url.clone(),
        settings.strategy.settlement.clone(),
    );
    poly_source.connect().await?;

    // Subscribe to all polymarket markets
    for market in &markets {
        poly_source.subscribe_market(&market.symbol).await?;
    }

    // Create CEX sources grouped by exchange
    let mut cex_sources: HashMap<Exchange, CexLiveSource> = HashMap::new();
    for market in &markets {
        if !cex_sources.contains_key(&market.hedge_exchange) {
            let source = build_cex_source(manager.clone(), market, &settings);
            cex_sources.insert(market.hedge_exchange, source);
        }
    }

    // Connect all CEX sources
    for source in cex_sources.values_mut() {
        source.connect().await?;
    }

    // Subscribe CEX sources to their markets
    for market in &markets {
        if let Some(source) = cex_sources.get_mut(&market.hedge_exchange) {
            source.subscribe_market(&market.symbol).await?;
        }
    }

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = build_paper_engine_config(&settings);
    let market_overrides = build_market_overrides(&settings, &markets);

    // Log per-market parameters
    if !market_overrides.is_empty() {
        println!("=== 分币种参数覆盖 ===");
        for market in &markets {
            let entry_z = market_overrides
                .get(&market.symbol)
                .and_then(|o| o.entry_z)
                .unwrap_or(engine_config.entry_z);
            println!(
                "  {} ({}) 入场Z = {}",
                market.symbol.0, market.cex_symbol, entry_z
            );
        }
        println!();
    }

    let mut engine = SimpleAlphaEngine::with_markets(engine_config.clone(), markets.clone());
    engine.set_market_overrides(market_overrides);
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor_start_ms = now_millis();
    let (orderbook_provider, executor, mut execution) = build_paper_execution_stack(
        &settings,
        &registry,
        executor_start_ms,
        execution_mode,
        None,
    )
    .await?;
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed: HashMap<Symbol, ObservedState> = markets
        .iter()
        .map(|m| (m.symbol.clone(), ObservedState::default()))
        .collect();
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut last_event_seq_sent = 0_u64;
    let mut snapshots = Vec::new();
    let mut trade_book = TradeBook::default();

    let start_time_ms = executor_start_ms;
    let mut audit = if settings.audit.enabled {
        let audit = PaperAudit::start(&settings, env, &markets, executor_start_ms, trading_mode)?;
        println!(
            "审计会话已启动：{} -> {}",
            audit.session_id(),
            audit.session_dir().display()
        );
        Some(audit)
    } else {
        None
    };

    if warmup_klines > 0 {
        for market in &markets {
            if let Some(source) = cex_sources.get(&market.hedge_exchange) {
                if let Err(err) =
                    warmup_market_engine(market, &mut engine, source, &poly_source, warmup_klines)
                        .await
                {
                    push_event_simple(
                        &mut recent_events,
                        "warmup",
                        format!("{} 预热失败：{}", market.symbol.0, err),
                    );
                }
            }
        }
    }

    loop {
        stats.ticks_processed += 1;
        let tick_index = stats.ticks_processed;
        let now_ms = now_millis();
        let now_secs = now_ms / 1000;

        drain_command_events(&command_center, &mut recent_events);
        let _ = process_pending_command_multi(
            &command_center,
            &paused,
            &emergency,
            &pre_emergency_paused,
            &mut settings,
            &markets,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut audit,
            now_ms,
        )
        .await?;
        drain_command_events(&command_center, &mut recent_events);

        // Publish lifecycle for all markets
        for market in &markets {
            if next_market_lifecycle_phase(
                observed.get(&market.symbol),
                market,
                now_secs,
                &settings.strategy.settlement,
            )
            .is_some()
            {
                let _ = manager.publish_market_lifecycle(
                    &market.symbol,
                    now_secs,
                    now_ms,
                    &settings.strategy.settlement,
                );
            } else if let Some(state) = observed.get_mut(&market.symbol) {
                state.market_phase = Some(current_market_lifecycle_phase(
                    market,
                    now_secs,
                    &settings.strategy.settlement,
                ));
            }
        }

        // Poll Polymarket for all subscribed markets
        let mut closed_markets: HashSet<Symbol> = HashSet::new();
        for market in &markets {
            for token_side in [TokenSide::Yes, TokenSide::No] {
                if let Err(err) = poly_source
                    .fetch_and_publish_orderbook_by_symbol(&market.symbol, token_side)
                    .await
                {
                    // Check if this is a 404 (market closed/settled) - don't count as error
                    let err_str = err.to_string();
                    if err_str.contains("404") || err_str.contains("No orderbook exists") {
                        // Market is likely closed/settled - log once per market, not per token
                        if token_side == TokenSide::Yes {
                            closed_markets.insert(market.symbol.clone());
                            push_event_simple(
                                &mut recent_events,
                                "market_closed",
                                format!("{} 疑似已关闭或已结算", market.symbol.0),
                            );
                        }
                    } else {
                        stats.poll_errors += 1;
                        push_event_simple(
                            &mut recent_events,
                            "poll_error",
                            format!(
                                "Polymarket {} 盘口轮询失败 {}：{err}",
                                format_token_side_label(token_side),
                                market.symbol.0
                            ),
                        );
                    }
                }
            }
        }

        // Handle single-leg CEX positions when Poly market is settled
        for symbol in &closed_markets {
            if risk.has_cex_position(symbol) {
                let cex_qty = risk.cex_position_qty(symbol);
                if !cex_qty.is_zero() {
                    push_event_simple(
                        &mut recent_events,
                        "emergency_cex_close",
                        format!(
                            "{} 市场已结算，准备平掉孤立交易所持仓（数量：{}）",
                            symbol.0, cex_qty
                        ),
                    );

                    let close_intent = close_intent_for_symbol(
                        symbol,
                        "market_settled",
                        &format!("emergency-{}-{}", symbol.0, now_ms),
                        now_ms,
                    );

                    match execution.process_intent(close_intent).await {
                        Ok(events) => {
                            for event in events {
                                push_event_simple(
                                    &mut recent_events,
                                    "emergency_fill",
                                    format!(
                                        "交易所紧急平仓：{}",
                                        summarize_execution_event(&event)
                                    ),
                                );
                                // Apply fills to risk manager
                                if let ExecutionEvent::OrderFilled(fill) = event {
                                    if let Err(e) = risk.on_fill(&fill).await {
                                        push_event_simple(
                                            &mut recent_events,
                                            "emergency_close_failed",
                                            format!("成交回填失败：{}", e),
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            push_event_simple(
                                &mut recent_events,
                                "emergency_close_failed",
                                format!("{} 的交易所持仓平仓失败：{}", symbol.0, e),
                            );
                        }
                    }
                    sync_engine_position_state_from_runtime(
                        &mut engine,
                        &risk,
                        &trade_book,
                        symbol,
                        None,
                    );
                }
            }
        }

        // Poll all CEX sources for their subscribed symbols
        for market in &markets {
            if let Some(source) = cex_sources.get_mut(&market.hedge_exchange) {
                if let Err(err) = source
                    .poll_symbol(&market.symbol, depth, include_funding)
                    .await
                {
                    stats.poll_errors += 1;
                    push_event_simple(
                        &mut recent_events,
                        "poll_error",
                        format!(
                            "{} 行情轮询失败 {}：{err}",
                            format_exchange_label(market.hedge_exchange),
                            market.symbol.0
                        ),
                    );
                }
            }
        }

        let connection_checked_at_ms = now_millis();
        for obs in observed.values_mut() {
            update_connection_state(
                obs,
                Exchange::Polymarket,
                poly_source.connection_status(),
                connection_checked_at_ms,
            );
        }
        for (exchange, source) in &cex_sources {
            for obs in observed.values_mut() {
                update_connection_state(
                    obs,
                    *exchange,
                    source.connection_status(),
                    connection_checked_at_ms,
                );
            }
        }

        loop {
            match market_data_rx.try_recv() {
                Ok(event) => {
                    handle_multi_market_event(
                        event,
                        &settings,
                        &registry,
                        &orderbook_provider,
                        &mut observed,
                        &mut stats,
                        &mut recent_events,
                        &mut engine,
                        &mut execution,
                        &mut risk,
                        &mut trade_book,
                        &paused,
                        &emergency,
                        false,
                        &mut audit,
                    )
                    .await?;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Lagged(_)) => continue,
                Err(TryRecvError::Closed) => break,
            }
        }

        // Build snapshot for all markets
        let snapshot = build_multi_snapshot(
            &markets, tick_index, &engine, &observed, &stats, &risk, &execution, &executor,
        )?;

        if tick_index % print_every.max(1) == 0 {
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
        snapshots.push(snapshot.clone());

        broadcast_monitor_events_since(
            &recent_events,
            &mut last_event_seq_sent,
            &event_broadcaster,
        );

        let monitor_client_count = *monitor_client_count.read().await;
        let has_monitor_clients = monitor_client_count > 0;
        if should_build_monitor_state(monitor_client_count, audit.as_ref(), now_ms) {
            let monitor_events = build_monitor_events(&recent_events);
            let monitor_state = build_multi_monitor_state(
                &settings,
                &markets,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_ms,
                &monitor_events,
                trading_mode,
            );

            if let Some(audit) = audit.as_mut() {
                audit.maybe_record_runtime_state(&stats, &monitor_state, tick_index, now_ms)?;
            }

            if has_monitor_clients {
                if let Ok(mut guard) = current_state.write() {
                    *guard = Some(monitor_state.clone());
                }
                let _ = state_broadcaster.send(monitor_state);
            } else if let Ok(mut guard) = current_state.write() {
                *guard = Some(monitor_state);
            }
        }

        if max_ticks > 0 && tick_index >= max_ticks {
            break;
        }
        if poll_interval_ms > 0 {
            sleep(Duration::from_millis(poll_interval_ms)).await;
        }
    }

    let final_monitor_state = current_state
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| {
            let monitor_events = build_monitor_events(&recent_events);
            build_multi_monitor_state(
                &settings,
                &markets,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_millis(),
                &monitor_events,
                trading_mode,
            )
        });
    if let Ok(mut guard) = current_state.write() {
        *guard = Some(final_monitor_state.clone());
    }
    let runtime_result: Result<()> = async {
        println!(
            "多市场模拟盘已完成，共处理 {} 个轮次。",
            stats.ticks_processed
        );
        let final_snapshot = snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("纸面盘会话未生成任何快照"))?;
        let open_orders = executor
            .open_order_snapshots()?
            .into_iter()
            .map(order_view_from_snapshot)
            .collect();
        let hedge_exchange = markets
            .first()
            .map(|market| format!("{:?}", market.hedge_exchange))
            .unwrap_or_else(|| "Unknown".to_owned());
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode,
            env: env.to_owned(),
            market_index: 0,
            market: format!("multi:{}", markets.len()),
            hedge_exchange,
            market_data_mode: Some("poll".to_owned()),
            poll_interval_ms,
            depth,
            include_funding,
            ticks_processed: stats.ticks_processed,
            final_snapshot,
            open_orders,
            recent_events: recent_events.into_iter().collect(),
            snapshots,
        };
        let artifact_path = artifact_path(&settings, env, trading_mode)?;
        persist_artifact(&artifact_path, &artifact)?;
        println!(
            "{}结果已写入：{}",
            artifact_runtime_label(trading_mode),
            artifact_path.display()
        );
        Ok(())
    }
    .await;
    finish_runtime_with_audit(
        runtime_result,
        &mut audit,
        &stats,
        &final_monitor_state,
        stats.ticks_processed,
        now_millis(),
    )?;
    if let Some(audit) = audit.as_ref() {
        println!("审计会话已完成：{}", audit.session_dir().display());
    }
    Ok(())
}

async fn bootstrap_single_market_snapshots(
    market: &MarketConfig,
    poly_source: &PolymarketLiveDataSource,
    cex_source: &CexLiveSource,
    depth: u16,
    include_funding: bool,
    recent_events: &mut VecDeque<PaperEventLog>,
    stats: &mut PaperStats,
) {
    if looks_like_placeholder_id(&market.poly_ids.yes_token_id)
        || looks_like_placeholder_id(&market.poly_ids.no_token_id)
    {
        push_event_simple(
            recent_events,
            "warning",
            "Polymarket token_id 仍是占位值，WS 模式已跳过 Polymarket 快照初始化".to_owned(),
        );
    } else {
        for token_side in [TokenSide::Yes, TokenSide::No] {
            if let Err(err) = poly_source
                .fetch_and_publish_orderbook_by_symbol(&market.symbol, token_side)
                .await
            {
                stats.poll_errors += 1;
                push_event_simple(
                    recent_events,
                    "poll_error",
                    format!(
                        "Polymarket {} 快照刷新失败：{err}",
                        format_token_side_label(token_side)
                    ),
                );
            }
        }
    }

    if let Err(err) = cex_source
        .poll_symbol(&market.symbol, depth, include_funding)
        .await
    {
        stats.poll_errors += 1;
        push_event_simple(
            recent_events,
            "poll_error",
            format!("交易所快照刷新失败：{err}"),
        );
    }
}

fn ws_housekeeping_interval(poll_interval_ms: u64) -> tokio::time::Interval {
    let interval_ms = poll_interval_ms.max(250);
    let mut timer = interval(Duration::from_millis(interval_ms));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer
}

type WsSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum WsProxyKind {
    Socks5,
    HttpConnect,
}

impl WsProxyKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Socks5 => "SOCKS5",
            Self::HttpConnect => "HTTP CONNECT",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WsProxyEndpoint {
    host: String,
    port: u16,
}

impl WsProxyEndpoint {
    fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WsProxyConfig {
    kind: WsProxyKind,
    endpoint: WsProxyEndpoint,
}

impl WsProxyConfig {
    fn describe(&self) -> String {
        format!("{} {}", self.kind.label(), self.endpoint.address())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SystemProxyConfig {
    socks: Option<WsProxyEndpoint>,
    https: Option<WsProxyEndpoint>,
    http: Option<WsProxyEndpoint>,
}

impl SystemProxyConfig {
    fn is_empty(&self) -> bool {
        self.socks.is_none() && self.https.is_none() && self.http.is_none()
    }
}

#[derive(Clone, Debug, Default)]
struct WsProxySettings {
    all_proxy: Option<String>,
    https_proxy: Option<String>,
    http_proxy: Option<String>,
    system_proxy: Option<SystemProxyConfig>,
}

impl WsProxySettings {
    fn detect() -> Self {
        Self {
            all_proxy: read_proxy_env(&["ALL_PROXY", "all_proxy"]),
            https_proxy: read_proxy_env(&["HTTPS_PROXY", "https_proxy"]),
            http_proxy: read_proxy_env(&["HTTP_PROXY", "http_proxy"]),
            system_proxy: load_system_proxy_config(),
        }
    }

    fn resolve_for_url(&self, ws_url: &str) -> Result<Option<WsProxyConfig>> {
        let target = parse_ws_target(ws_url)?;
        select_ws_proxy(
            &target.scheme,
            self.all_proxy.as_deref(),
            self.https_proxy.as_deref(),
            self.http_proxy.as_deref(),
            self.system_proxy.as_ref(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WsConnectTarget {
    scheme: String,
    host: String,
    port: u16,
}

impl WsConnectTarget {
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn read_proxy_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        env::var(name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn parse_proxy_url(raw: &str) -> Result<WsProxyConfig> {
    let raw = raw.trim();
    let url = Url::parse(raw).with_context(|| format!("invalid proxy URL `{raw}`"))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(anyhow!(
            "proxy authentication is not supported for websocket transport"
        ));
    }

    let host = url
        .host_str()
        .context("proxy URL is missing host")?
        .to_owned();
    let port = url
        .port_or_known_default()
        .context("proxy URL is missing port")?;
    let kind = match url.scheme() {
        "socks5" | "socks5h" => WsProxyKind::Socks5,
        "http" => WsProxyKind::HttpConnect,
        "https" => {
            return Err(anyhow!(
                "https proxy URLs are not supported; use http:// or socks5://"
            ))
        }
        other => return Err(anyhow!("unsupported proxy scheme `{other}`")),
    };

    Ok(WsProxyConfig {
        kind,
        endpoint: WsProxyEndpoint { host, port },
    })
}

fn parse_scutil_proxy_config(raw: &str) -> SystemProxyConfig {
    let mut values = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if let Some((key, value)) = line.split_once(':') {
            values.insert(key.trim().to_owned(), value.trim().to_owned());
        }
    }

    SystemProxyConfig {
        socks: parse_scutil_proxy_endpoint("SOCKS", &values),
        https: parse_scutil_proxy_endpoint("HTTPS", &values),
        http: parse_scutil_proxy_endpoint("HTTP", &values),
    }
}

fn parse_scutil_proxy_endpoint(
    prefix: &str,
    values: &HashMap<String, String>,
) -> Option<WsProxyEndpoint> {
    let enabled = values
        .get(&format!("{prefix}Enable"))
        .is_some_and(|value| value == "1");
    if !enabled {
        return None;
    }

    let host = values.get(&format!("{prefix}Proxy"))?.trim();
    if host.is_empty() {
        return None;
    }

    let port = values.get(&format!("{prefix}Port"))?.parse().ok()?;
    Some(WsProxyEndpoint {
        host: host.to_owned(),
        port,
    })
}

#[cfg(target_os = "macos")]
fn load_system_proxy_config() -> Option<SystemProxyConfig> {
    let output = Command::new("scutil").arg("--proxy").output().ok()?;
    if !output.status.success() {
        return None;
    }

    let config = parse_scutil_proxy_config(&String::from_utf8_lossy(&output.stdout));
    (!config.is_empty()).then_some(config)
}

#[cfg(not(target_os = "macos"))]
fn load_system_proxy_config() -> Option<SystemProxyConfig> {
    None
}

fn select_ws_proxy(
    scheme: &str,
    env_all_proxy: Option<&str>,
    env_https_proxy: Option<&str>,
    env_http_proxy: Option<&str>,
    system_proxy: Option<&SystemProxyConfig>,
) -> Result<Option<WsProxyConfig>> {
    let env_proxy = match scheme {
        "wss" => env_all_proxy.or(env_https_proxy),
        "ws" => env_all_proxy.or(env_http_proxy),
        other => return Err(anyhow!("unsupported websocket scheme `{other}`")),
    };

    if let Some(raw) = env_proxy {
        return parse_proxy_url(raw).map(Some);
    }

    let Some(system_proxy) = system_proxy else {
        return Ok(None);
    };

    if let Some(endpoint) = system_proxy.socks.as_ref() {
        return Ok(Some(WsProxyConfig {
            kind: WsProxyKind::Socks5,
            endpoint: endpoint.clone(),
        }));
    }

    if scheme == "wss" {
        if let Some(endpoint) = system_proxy.https.as_ref() {
            return Ok(Some(WsProxyConfig {
                kind: WsProxyKind::HttpConnect,
                endpoint: endpoint.clone(),
            }));
        }
    }

    if let Some(endpoint) = system_proxy.http.as_ref() {
        return Ok(Some(WsProxyConfig {
            kind: WsProxyKind::HttpConnect,
            endpoint: endpoint.clone(),
        }));
    }

    Ok(None)
}

fn parse_ws_target(ws_url: &str) -> Result<WsConnectTarget> {
    let url = Url::parse(ws_url).with_context(|| format!("invalid websocket URL `{ws_url}`"))?;
    let scheme = url.scheme().to_owned();
    if scheme != "ws" && scheme != "wss" {
        return Err(anyhow!("unsupported websocket scheme `{scheme}`"));
    }

    let host = url
        .host_str()
        .context("websocket URL is missing host")?
        .to_owned();
    let port = url
        .port_or_known_default()
        .context("websocket URL is missing port")?;

    Ok(WsConnectTarget { scheme, host, port })
}

fn ws_transport_error(message: impl Into<String>) -> tungstenite::Error {
    tungstenite::Error::Io(std::io::Error::other(message.into()))
}

fn build_ws_request_with_optional_extensions(
    ws_url: &str,
    extensions: Option<&str>,
) -> Result<tungstenite::http::Request<()>> {
    let mut request = ws_url.into_client_request()?;
    if let Some(extensions) = extensions {
        request.headers_mut().insert(
            "Sec-WebSocket-Extensions",
            HeaderValue::from_str(extensions)?,
        );
    }
    Ok(request)
}

fn negotiated_ws_extensions<B>(response: &tungstenite::http::Response<B>) -> Option<String> {
    response
        .headers()
        .get("Sec-WebSocket-Extensions")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

async fn connect_websocket(
    ws_url: &str,
    proxy: Option<&WsProxyConfig>,
) -> std::result::Result<
    (
        WsSocket,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    connect_websocket_with_extensions(ws_url, proxy, None).await
}

async fn connect_websocket_with_extensions(
    ws_url: &str,
    proxy: Option<&WsProxyConfig>,
    extensions: Option<&str>,
) -> std::result::Result<
    (
        WsSocket,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    let target = parse_ws_target(ws_url).map_err(|err| ws_transport_error(err.to_string()))?;
    let stream = match proxy {
        Some(proxy) => connect_via_proxy(&target, proxy).await?,
        None => TcpStream::connect((target.host.as_str(), target.port))
            .await
            .map_err(|err| {
                ws_transport_error(format!(
                    "tcp connect failed for {}: {err}",
                    target.authority()
                ))
            })?,
    };

    let request = build_ws_request_with_optional_extensions(ws_url, extensions)
        .map_err(|err| ws_transport_error(err.to_string()))?;
    client_async_tls_with_config(request, stream, None, None).await
}

async fn connect_via_proxy(
    target: &WsConnectTarget,
    proxy: &WsProxyConfig,
) -> std::result::Result<TcpStream, tungstenite::Error> {
    let mut stream = TcpStream::connect((proxy.endpoint.host.as_str(), proxy.endpoint.port))
        .await
        .map_err(|err| {
            ws_transport_error(format!(
                "proxy tcp connect failed for {}: {err}",
                proxy.endpoint.address()
            ))
        })?;

    match proxy.kind {
        WsProxyKind::Socks5 => socks5_connect(&mut stream, target, proxy).await?,
        WsProxyKind::HttpConnect => http_connect_tunnel(&mut stream, target, proxy).await?,
    }

    Ok(stream)
}

async fn socks5_connect(
    stream: &mut TcpStream,
    target: &WsConnectTarget,
    proxy: &WsProxyConfig,
) -> std::result::Result<(), tungstenite::Error> {
    stream.write_all(&[0x05, 0x01, 0x00]).await.map_err(|err| {
        ws_transport_error(format!("{} auth write failed: {err}", proxy.describe()))
    })?;

    let mut method_response = [0_u8; 2];
    stream
        .read_exact(&mut method_response)
        .await
        .map_err(|err| {
            ws_transport_error(format!("{} auth read failed: {err}", proxy.describe()))
        })?;
    if method_response != [0x05, 0x00] {
        return Err(ws_transport_error(format!(
            "{} rejected no-auth SOCKS5 handshake: version={} method={}",
            proxy.describe(),
            method_response[0],
            method_response[1]
        )));
    }

    let mut request = vec![0x05, 0x01, 0x00];
    match IpAddr::from_str(&target.host) {
        Ok(IpAddr::V4(address)) => {
            request.push(0x01);
            request.extend_from_slice(&address.octets());
        }
        Ok(IpAddr::V6(address)) => {
            request.push(0x04);
            request.extend_from_slice(&address.octets());
        }
        Err(_) => {
            let host = target.host.as_bytes();
            if host.len() > u8::MAX as usize {
                return Err(ws_transport_error(format!(
                    "SOCKS5 target host is too long: {}",
                    target.host
                )));
            }
            request.push(0x03);
            request.push(host.len() as u8);
            request.extend_from_slice(host);
        }
    }
    request.extend_from_slice(&target.port.to_be_bytes());

    stream.write_all(&request).await.map_err(|err| {
        ws_transport_error(format!("{} connect write failed: {err}", proxy.describe()))
    })?;

    let mut response_header = [0_u8; 4];
    stream
        .read_exact(&mut response_header)
        .await
        .map_err(|err| {
            ws_transport_error(format!("{} connect read failed: {err}", proxy.describe()))
        })?;
    if response_header[0] != 0x05 {
        return Err(ws_transport_error(format!(
            "{} returned invalid SOCKS version {}",
            proxy.describe(),
            response_header[0]
        )));
    }
    if response_header[1] != 0x00 {
        return Err(ws_transport_error(format!(
            "{} rejected CONNECT with reply code {}",
            proxy.describe(),
            response_header[1]
        )));
    }

    let address_length = match response_header[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length).await.map_err(|err| {
                ws_transport_error(format!(
                    "{} reply length read failed: {err}",
                    proxy.describe()
                ))
            })?;
            length[0] as usize
        }
        atyp => {
            return Err(ws_transport_error(format!(
                "{} returned unsupported address type {atyp}",
                proxy.describe()
            )))
        }
    };

    let mut discard = vec![0_u8; address_length + 2];
    stream.read_exact(&mut discard).await.map_err(|err| {
        ws_transport_error(format!(
            "{} reply tail read failed: {err}",
            proxy.describe()
        ))
    })?;

    Ok(())
}

async fn http_connect_tunnel(
    stream: &mut TcpStream,
    target: &WsConnectTarget,
    proxy: &WsProxyConfig,
) -> std::result::Result<(), tungstenite::Error> {
    let authority = target.authority();
    let request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.map_err(|err| {
        ws_transport_error(format!("{} CONNECT write failed: {err}", proxy.describe()))
    })?;

    let mut response = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await.map_err(|err| {
            ws_transport_error(format!("{} CONNECT read failed: {err}", proxy.describe()))
        })?;
        if read == 0 {
            return Err(ws_transport_error(format!(
                "{} closed before CONNECT completed",
                proxy.describe()
            )));
        }
        response.extend_from_slice(&buffer[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > 8 * 1024 {
            return Err(ws_transport_error(format!(
                "{} CONNECT response exceeded 8KiB",
                proxy.describe()
            )));
        }
    }

    let response_text = String::from_utf8_lossy(&response);
    let status_line = response_text.lines().next().unwrap_or_default();
    let status_code = status_line.split_whitespace().nth(1).unwrap_or_default();
    if status_code != "200" {
        return Err(ws_transport_error(format!(
            "{} CONNECT failed: {}",
            proxy.describe(),
            status_line
        )));
    }

    Ok(())
}

fn spawn_ws_market_data_tasks(
    manager: DataManager,
    settings: &Settings,
    markets: &[MarketConfig],
    depth: u16,
    polymarket_ws_diagnostics: PolymarketWsDiagnostics,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    let reconnect_backoff_ms = settings.strategy.market_data.reconnect_backoff_ms.max(250);
    let proxy_settings = WsProxySettings::detect();

    let poly_asset_ids: Vec<String> = markets
        .iter()
        .filter(|market| {
            !looks_like_placeholder_id(&market.poly_ids.yes_token_id)
                && !looks_like_placeholder_id(&market.poly_ids.no_token_id)
        })
        .flat_map(|market| {
            [
                market.poly_ids.yes_token_id.clone(),
                market.poly_ids.no_token_id.clone(),
            ]
        })
        .collect();
    if !poly_asset_ids.is_empty() {
        handles.push(spawn_polymarket_ws_task(
            manager.clone(),
            settings.polymarket.ws_url.clone(),
            poly_asset_ids,
            reconnect_backoff_ms,
            proxy_settings.clone(),
            polymarket_ws_diagnostics,
        ));
    }

    let binance_tracker = ExchangeConnectionTracker::new(Exchange::Binance, manager.clone());
    let okx_tracker = ExchangeConnectionTracker::new(Exchange::Okx, manager.clone());
    let mut seen_binance = HashSet::new();
    let mut seen_okx = HashSet::new();
    let mut binance_symbols = Vec::new();

    for market in markets {
        let venue_symbol = cex_venue_symbol(market.hedge_exchange, &market.cex_symbol);
        match market.hedge_exchange {
            Exchange::Binance if seen_binance.insert(venue_symbol.clone()) => {
                binance_symbols.push(venue_symbol);
            }
            Exchange::Okx if seen_okx.insert(venue_symbol.clone()) => {
                handles.push(spawn_okx_ws_task(
                    manager.clone(),
                    settings.okx.ws_public_url.clone(),
                    venue_symbol,
                    reconnect_backoff_ms,
                    okx_tracker.clone(),
                    proxy_settings.clone(),
                ));
            }
            _ => {}
        }
    }

    match select_binance_ws_transport_mode(settings) {
        BinanceWsTransportMode::PerSymbol => {
            for venue_symbol in binance_symbols {
                handles.push(spawn_binance_ws_task(
                    manager.clone(),
                    settings.binance.ws_url.clone(),
                    venue_symbol,
                    depth,
                    reconnect_backoff_ms,
                    binance_tracker.clone(),
                    proxy_settings.clone(),
                ));
            }
        }
        BinanceWsTransportMode::Combined if !binance_symbols.is_empty() => {
            handles.push(spawn_binance_combined_ws_task(
                manager.clone(),
                settings.binance.ws_url.clone(),
                binance_symbols,
                depth,
                reconnect_backoff_ms,
                binance_tracker,
                proxy_settings,
            ));
        }
        BinanceWsTransportMode::Combined => {}
    }

    handles
}

fn spawn_polymarket_ws_task(
    manager: DataManager,
    ws_url: String,
    asset_ids: Vec<String>,
    reconnect_backoff_ms: u64,
    proxy_settings: WsProxySettings,
    diagnostics: PolymarketWsDiagnostics,
) -> JoinHandle<()> {
    let tracker = ExchangeConnectionTracker::new(Exchange::Polymarket, manager.clone());
    let proxy_resolution = proxy_settings.resolve_for_url(&ws_url);
    if let Ok(Some(proxy)) = proxy_resolution.as_ref() {
        eprintln!("polymarket ws using {}", proxy.describe());
    } else if let Err(err) = proxy_resolution.as_ref() {
        log_ws_warning(format!("polymarket ws proxy resolution failed: {err}"));
    }
    tokio::spawn(async move {
        let stream_key = "market".to_owned();
        let books: Arc<Mutex<HashMap<String, LocalPolyBook>>> =
            Arc::new(Mutex::new(HashMap::new()));
        tracker.mark_connecting();
        let proxy = match proxy_resolution {
            Ok(proxy) => proxy,
            Err(_) => {
                tracker.mark_disconnected(&stream_key);
                return;
            }
        };

        loop {
            sleep(Duration::from_millis(ws_connect_delay_ms(
                reconnect_backoff_ms,
                &format!("polymarket:{stream_key}"),
            )))
            .await;
            tracker.mark_connecting();
            match run_ws_future_with_timeout(
                connect_websocket(ws_url.as_str(), proxy.as_ref()),
                Duration::from_millis(WS_CONNECT_TIMEOUT_MS),
                "polymarket ws connect".to_owned(),
            )
            .await
            {
                Ok((socket, _)) => {
                    let (mut writer, mut reader) = socket.split();
                    let subscribe =
                        PolymarketLiveDataSource::build_orderbook_subscribe_message(&asset_ids);
                    if let Err(err) = writer
                        .send(Message::Text(subscribe.to_string().into()))
                        .await
                    {
                        log_ws_warning(format!("polymarket ws subscribe failed: {err}"));
                        tracker.mark_disconnected(&stream_key);
                        continue;
                    }
                    tracker.mark_connected(&stream_key);
                    eprintln!("polymarket ws connected");

                    let mut heartbeat = interval(Duration::from_secs(10));
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if let Err(err) = writer.send(Message::Text("PING".to_owned().into())).await {
                                    log_ws_warning(format!("polymarket ws heartbeat failed: {err}"));
                                    break;
                                }
                            }
                            message = reader.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        tracker.reaffirm_connected();
                                        diagnostics.record_text_frame();
                                        if text == "PONG" {
                                            continue;
                                        }
                                        if let Err(err) = handle_polymarket_ws_text(
                                            &manager,
                                            &books,
                                            &diagnostics,
                                            &text,
                                        )
                                        .await
                                        {
                                            log_ws_warning(format!("polymarket ws payload ignored: {err}"));
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        tracker.reaffirm_connected();
                                        if writer.send(Message::Pong(payload)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Pong(_))) => {
                                        tracker.reaffirm_connected();
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        log_ws_warning(format!(
                                            "polymarket ws closed: {}",
                                            format_ws_close_reason(frame.as_ref())
                                        ));
                                        break;
                                    }
                                    None => {
                                        log_ws_warning("polymarket ws stream ended without close frame");
                                        break;
                                    }
                                    Some(Err(err)) => {
                                        log_ws_warning(format!("polymarket ws read error: {err}"));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(err) => log_ws_warning(format!("polymarket ws connect failed: {err}")),
            }

            tracker.mark_disconnected(&stream_key);
        }
    })
}

fn spawn_binance_ws_task(
    manager: DataManager,
    ws_url: String,
    venue_symbol: String,
    depth: u16,
    reconnect_backoff_ms: u64,
    tracker: ExchangeConnectionTracker,
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let stream_name =
        BinanceFuturesDataSource::build_partial_depth_stream_name(&venue_symbol, depth);
    let endpoint = format!("{}/ws/{}", ws_url.trim_end_matches('/'), stream_name);
    let proxy_resolution = proxy_settings.resolve_for_url(&endpoint);
    if let Ok(Some(proxy)) = proxy_resolution.as_ref() {
        eprintln!("binance ws using {} for {}", proxy.describe(), venue_symbol);
    } else if let Err(err) = proxy_resolution.as_ref() {
        log_ws_warning(format!(
            "binance ws proxy resolution failed for {}: {}",
            venue_symbol, err
        ));
    }
    tokio::spawn(async move {
        tracker.mark_connecting();
        let proxy = match proxy_resolution {
            Ok(proxy) => proxy,
            Err(_) => {
                tracker.mark_disconnected(&venue_symbol);
                return;
            }
        };

        loop {
            sleep(Duration::from_millis(ws_connect_delay_ms(
                reconnect_backoff_ms,
                &format!("binance:{venue_symbol}"),
            )))
            .await;
            tracker.mark_connecting();
            match run_ws_future_with_timeout(
                connect_websocket(endpoint.as_str(), proxy.as_ref()),
                Duration::from_millis(WS_CONNECT_TIMEOUT_MS),
                format!("binance ws connect for {venue_symbol}"),
            )
            .await
            {
                Ok((socket, _)) => {
                    let (_writer, mut reader) = socket.split();
                    tracker.mark_connected(&venue_symbol);
                    eprintln!("binance ws connected for {venue_symbol}");

                    while let Some(message) = reader.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                tracker.reaffirm_connected();
                                match BinanceFuturesDataSource::parse_partial_depth_ws_payload(
                                    &text,
                                    &venue_symbol,
                                ) {
                                    Ok(Some(update)) => {
                                        if let Err(err) =
                                            manager.normalize_and_publish_cex_orderbook(update)
                                        {
                                            log_ws_warning(format!(
                                                "binance ws publish failed for {}: {}",
                                                venue_symbol, err
                                            ));
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(err) => log_ws_warning(format!(
                                        "binance ws payload ignored for {}: {}",
                                        venue_symbol, err
                                    )),
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                log_ws_warning(format!(
                                    "binance ws closed for {}: {}",
                                    venue_symbol,
                                    format_ws_close_reason(frame.as_ref())
                                ));
                                break;
                            }
                            Err(err) => {
                                log_ws_warning(format!(
                                    "binance ws read error for {}: {}",
                                    venue_symbol, err
                                ));
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => log_ws_warning(format!(
                    "binance ws connect failed for {}: {}",
                    venue_symbol, err
                )),
            }

            tracker.mark_disconnected(&venue_symbol);
        }
    })
}

async fn fallback_to_binance_per_symbol_ws(
    manager: DataManager,
    ws_url: String,
    venue_symbols: Vec<String>,
    depth: u16,
    reconnect_backoff_ms: u64,
    tracker: ExchangeConnectionTracker,
    proxy_settings: WsProxySettings,
) {
    log_ws_warning("binance combined ws failed; falling back to per-symbol transport");
    for venue_symbol in &venue_symbols {
        tracker.mark_disconnected(venue_symbol);
    }

    let mut fallback_handles = venue_symbols
        .into_iter()
        .map(|venue_symbol| {
            spawn_binance_ws_task(
                manager.clone(),
                ws_url.clone(),
                venue_symbol,
                depth,
                reconnect_backoff_ms,
                tracker.clone(),
                proxy_settings.clone(),
            )
        })
        .collect::<Vec<_>>();

    for handle in fallback_handles.drain(..) {
        let _ = handle.await;
    }
}

fn spawn_binance_combined_ws_task(
    manager: DataManager,
    ws_url: String,
    venue_symbols: Vec<String>,
    depth: u16,
    reconnect_backoff_ms: u64,
    tracker: ExchangeConnectionTracker,
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let stream_path =
        BinanceFuturesDataSource::build_combined_partial_depth_stream_path(&venue_symbols, depth);
    let endpoint = format!("{}{}", ws_url.trim_end_matches('/'), stream_path);
    let proxy_resolution = proxy_settings.resolve_for_url(&endpoint);
    if let Ok(Some(proxy)) = proxy_resolution.as_ref() {
        eprintln!(
            "binance combined ws using {} for {} symbols",
            proxy.describe(),
            venue_symbols.len()
        );
    } else if let Err(err) = proxy_resolution.as_ref() {
        log_ws_warning(format!(
            "binance combined ws proxy resolution failed: {err}"
        ));
    }

    tokio::spawn(async move {
        tracker.mark_connecting();

        let proxy = match proxy_resolution {
            Ok(proxy) => proxy,
            Err(_) => {
                fallback_to_binance_per_symbol_ws(
                    manager.clone(),
                    ws_url.clone(),
                    venue_symbols.clone(),
                    depth,
                    reconnect_backoff_ms,
                    tracker.clone(),
                    proxy_settings.clone(),
                )
                .await;
                return;
            }
        };

        sleep(Duration::from_millis(ws_connect_delay_ms(
            reconnect_backoff_ms,
            "binance:combined",
        )))
        .await;
        tracker.mark_connecting();

        match run_ws_future_with_timeout(
            connect_websocket_with_extensions(
                endpoint.as_str(),
                proxy.as_ref(),
                binance_combined_ws_requested_extensions(),
            ),
            Duration::from_millis(WS_CONNECT_TIMEOUT_MS),
            "binance combined ws connect".to_owned(),
        )
        .await
        {
            Ok((socket, response)) => {
                if let Some(extensions) = negotiated_ws_extensions(&response) {
                    eprintln!("binance combined ws negotiated extensions: {extensions}");
                } else {
                    eprintln!(
                        "binance combined ws connected without negotiated extensions (client requested none)"
                    );
                }

                let (_writer, mut reader) = socket.split();
                for venue_symbol in &venue_symbols {
                    tracker.mark_connected(venue_symbol);
                }
                eprintln!(
                    "binance combined ws connected for {} symbols",
                    venue_symbols.len()
                );

                loop {
                    match reader.next().await {
                        Some(Ok(Message::Text(text))) => {
                            tracker.reaffirm_connected();
                            match BinanceFuturesDataSource::parse_partial_depth_ws_payload(
                                &text, "",
                            ) {
                                Ok(Some(update)) => {
                                    if let Err(err) =
                                        manager.normalize_and_publish_cex_orderbook(update)
                                    {
                                        log_ws_warning(format!(
                                            "binance combined ws publish failed: {err}"
                                        ));
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => log_ws_warning(format!(
                                    "binance combined ws payload ignored: {err}"
                                )),
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            log_ws_warning(format!(
                                "binance combined ws closed: {}",
                                format_ws_close_reason(frame.as_ref())
                            ));
                            break;
                        }
                        Some(Err(err)) => {
                            log_ws_warning(format!("binance combined ws read error: {err}"));
                            break;
                        }
                        None => {
                            log_ws_warning("binance combined ws stream ended without close frame");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => log_ws_warning(format!("binance combined ws connect failed: {err}")),
        }

        fallback_to_binance_per_symbol_ws(
            manager,
            ws_url,
            venue_symbols,
            depth,
            reconnect_backoff_ms,
            tracker,
            proxy_settings,
        )
        .await;
    })
}

fn spawn_okx_ws_task(
    manager: DataManager,
    ws_url: String,
    venue_symbol: String,
    reconnect_backoff_ms: u64,
    tracker: ExchangeConnectionTracker,
    proxy_settings: WsProxySettings,
) -> JoinHandle<()> {
    let proxy_resolution = proxy_settings.resolve_for_url(&ws_url);
    if let Ok(Some(proxy)) = proxy_resolution.as_ref() {
        eprintln!("okx ws using {} for {}", proxy.describe(), venue_symbol);
    } else if let Err(err) = proxy_resolution.as_ref() {
        log_ws_warning(format!(
            "okx ws proxy resolution failed for {}: {}",
            venue_symbol, err
        ));
    }
    tokio::spawn(async move {
        tracker.mark_connecting();
        let proxy = match proxy_resolution {
            Ok(proxy) => proxy,
            Err(_) => {
                tracker.mark_disconnected(&venue_symbol);
                return;
            }
        };
        loop {
            sleep(Duration::from_millis(ws_connect_delay_ms(
                reconnect_backoff_ms,
                &format!("okx:{venue_symbol}"),
            )))
            .await;
            tracker.mark_connecting();
            match run_ws_future_with_timeout(
                connect_websocket(ws_url.as_str(), proxy.as_ref()),
                Duration::from_millis(WS_CONNECT_TIMEOUT_MS),
                format!("okx ws connect for {venue_symbol}"),
            )
            .await
            {
                Ok((socket, _)) => {
                    let (mut writer, mut reader) = socket.split();
                    let subscribe =
                        OkxMarketDataSource::build_books_subscribe_message(&venue_symbol);
                    if let Err(err) = writer
                        .send(Message::Text(subscribe.to_string().into()))
                        .await
                    {
                        log_ws_warning(format!(
                            "okx ws subscribe failed for {}: {}",
                            venue_symbol, err
                        ));
                        tracker.mark_disconnected(&venue_symbol);
                        continue;
                    }
                    tracker.mark_connected(&venue_symbol);
                    eprintln!("okx ws connected for {venue_symbol}");

                    let mut heartbeat = interval(Duration::from_secs(20));
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if let Err(err) = writer.send(Message::Text("ping".to_owned().into())).await {
                                    log_ws_warning(format!(
                                        "okx ws heartbeat failed for {}: {}",
                                        venue_symbol, err
                                    ));
                                    break;
                                }
                            }
                            message = reader.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        tracker.reaffirm_connected();
                                        if text.eq_ignore_ascii_case("pong") {
                                            continue;
                                        }
                                        match OkxMarketDataSource::parse_orderbook_ws_message(&text, &venue_symbol) {
                                            Ok(Some(update)) => {
                                                if let Err(err) = manager.normalize_and_publish_cex_orderbook(update) {
                                                    log_ws_warning(format!(
                                                        "okx ws publish failed for {}: {}",
                                                        venue_symbol, err
                                                    ));
                                                }
                                            }
                                            Ok(None) => {}
                                            Err(err) => log_ws_warning(format!(
                                                "okx ws payload ignored for {}: {}",
                                                venue_symbol, err
                                            )),
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        tracker.reaffirm_connected();
                                        if writer.send(Message::Pong(payload)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Pong(_))) => {
                                        tracker.reaffirm_connected();
                                    }
                                    Some(Ok(Message::Close(frame))) => {
                                        log_ws_warning(format!(
                                            "okx ws closed for {}: {}",
                                            venue_symbol,
                                            format_ws_close_reason(frame.as_ref())
                                        ));
                                        break;
                                    }
                                    None => {
                                        log_ws_warning(format!(
                                            "okx ws stream ended for {} without close frame",
                                            venue_symbol
                                        ));
                                        break;
                                    }
                                    Some(Err(err)) => {
                                        log_ws_warning(format!(
                                            "okx ws read error for {}: {}",
                                            venue_symbol, err
                                        ));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(err) => log_ws_warning(format!(
                    "okx ws connect failed for {}: {}",
                    venue_symbol, err
                )),
            }

            tracker.mark_disconnected(&venue_symbol);
        }
    })
}

async fn handle_polymarket_ws_text(
    manager: &DataManager,
    books: &Arc<Mutex<HashMap<String, LocalPolyBook>>>,
    diagnostics: &PolymarketWsDiagnostics,
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
            diagnostics.record_price_change_message();
            let changes = parse_polymarket_price_changes(&message)?;
            if !changes.is_empty() {
                let emitted_updates = apply_polymarket_price_changes(manager, books, &changes)?;
                diagnostics.add_orderbook_updates(emitted_updates as u64);
            }
            continue;
        }

        if let Some(last_trade_price) = message
            .get("last_trade_price")
            .or_else(|| message.get("price"))
            .and_then(|value| parse_decimal_value(Some(value)))
        {
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
            diagnostics.record_book_message();
            let mut update =
                PolymarketLiveDataSource::parse_ws_orderbook_payload(&message.to_string(), "")?;
            if let Ok(mut guard) = books.lock() {
                let previous_sequence = guard
                    .get(&update.asset_id)
                    .map(|book| book.sequence)
                    .unwrap_or(0);
                let book = LocalPolyBook::from_update_with_previous(&update, previous_sequence);
                update.sequence = book.sequence;
                guard.insert(update.asset_id.clone(), book);
            }
            let _ = manager.normalize_and_publish_poly_orderbook(update)?;
            diagnostics.add_orderbook_updates(1);
        }
    }

    Ok(())
}

fn apply_polymarket_price_changes(
    manager: &DataManager,
    books: &Arc<Mutex<HashMap<String, LocalPolyBook>>>,
    changes: &[PolymarketPriceChange],
) -> Result<usize> {
    let mut grouped: HashMap<String, (u64, LocalPolyBook)> = HashMap::new();
    let mut guard = books
        .lock()
        .map_err(|_| anyhow!("Polymarket 订单簿缓存锁异常"))?;

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

fn parse_polymarket_price_changes(message: &Value) -> Result<Vec<PolymarketPriceChange>> {
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
    let default_timestamp_ms = parse_timestamp_value(
        message
            .get("timestamp")
            .or_else(|| message.get("ts"))
            .or_else(|| message.get("created_at")),
    )
    .unwrap_or_else(now_millis);

    changes
        .into_iter()
        .map(|change| {
            let asset_id = change
                .get("asset_id")
                .or_else(|| change.get("market"))
                .and_then(Value::as_str)
                .map(|value| value.to_owned())
                .or_else(|| default_asset_id.clone())
                .ok_or_else(|| anyhow!("Polymarket price_change 缺少 asset_id"))?;
            let price = parse_decimal_value(
                change
                    .get("price")
                    .or_else(|| change.get("px"))
                    .or_else(|| change.get("p")),
            )
            .ok_or_else(|| anyhow!("Polymarket price_change 缺少 price"))?;
            let size = parse_decimal_value(
                change
                    .get("size")
                    .or_else(|| change.get("qty"))
                    .or_else(|| change.get("quantity"))
                    .or_else(|| change.get("s")),
            )
            .ok_or_else(|| anyhow!("Polymarket price_change 缺少 size"))?;
            let side = parse_polymarket_side(
                change
                    .get("side")
                    .or_else(|| change.get("book_side"))
                    .and_then(Value::as_str),
            )
            .ok_or_else(|| anyhow!("Polymarket price_change 缺少 side"))?;
            let timestamp_ms = parse_timestamp_value(
                change
                    .get("timestamp")
                    .or_else(|| change.get("ts"))
                    .or_else(|| change.get("created_at")),
            )
            .unwrap_or(default_timestamp_ms);

            Ok(PolymarketPriceChange {
                asset_id,
                price,
                size,
                side,
                timestamp_ms,
            })
        })
        .collect()
}

fn parse_polymarket_side(value: Option<&str>) -> Option<PolymarketBookSide> {
    match value?.to_ascii_uppercase().as_str() {
        "BUY" | "BID" => Some(PolymarketBookSide::Bid),
        "SELL" | "ASK" => Some(PolymarketBookSide::Ask),
        _ => None,
    }
}

fn parse_decimal_value(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(raw) => Decimal::from_str(raw).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn parse_timestamp_value(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(raw)) => raw.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
fn ws_candidate_stale_reason(
    candidate: &OpenCandidate,
    observed: &ObservedState,
    max_stale_ms: u64,
) -> Option<String> {
    let now_ms = now_millis();
    let Some(cex_updated_at_ms) = observed.cex_updated_at_ms else {
        return Some("missing_cex".to_owned());
    };
    if now_ms.saturating_sub(cex_updated_at_ms) > max_stale_ms {
        return Some("cex_stale".to_owned());
    }

    let poly_updated_at_ms = match candidate.token_side {
        TokenSide::Yes => observed.poly_yes_updated_at_ms,
        TokenSide::No => observed.poly_no_updated_at_ms,
    };
    let Some(poly_updated_at_ms) = poly_updated_at_ms else {
        return Some("missing_poly".to_owned());
    };
    if now_ms.saturating_sub(poly_updated_at_ms) > max_stale_ms {
        return Some("poly_stale".to_owned());
    }

    None
}

fn format_ws_freshness_reason(reason: &str) -> &'static str {
    match reason {
        "bootstrap_in_progress" => "WS 启动补齐尚未完成",
        "missing_cex" => "缺少交易所最新行情",
        "cex_stale" => "交易所行情已超时",
        "missing_poly" => "缺少 Polymarket 最新行情",
        "poly_stale" => "Polymarket 行情已超时",
        "misaligned" => "双腿时间未对齐",
        "connection_impaired" => "行情连接异常",
        "poly_lag_borderline" => "慢腿处于临界区间",
        "missing_observed_state" => "缺少市场观测状态",
        _ => "行情新鲜度检查未通过",
    }
}

fn ws_signal_gate_reason(observed: &AuditObservedMarket) -> Option<&'static str> {
    match observed.evaluable_status {
        EvaluableStatus::Unknown => Some("missing_observed_state"),
        EvaluableStatus::Evaluable => match observed.async_classification {
            AsyncClassification::PolyLagBorderline => Some("poly_lag_borderline"),
            _ => None,
        },
        EvaluableStatus::NotEvaluableNoPoly => Some("missing_poly"),
        EvaluableStatus::NotEvaluableNoCex => Some("missing_cex"),
        EvaluableStatus::NotEvaluableMisaligned => Some("misaligned"),
        EvaluableStatus::ConnectionImpaired => Some("connection_impaired"),
        EvaluableStatus::PolyQuoteStale => Some("poly_stale"),
        EvaluableStatus::CexQuoteStale => Some("cex_stale"),
    }
}

fn ws_bootstrap_gate_reason(
    bootstrap_in_flight: bool,
    observed: Option<&AuditObservedMarket>,
) -> Option<&'static str> {
    if !bootstrap_in_flight {
        return None;
    }

    observed
        .filter(|snapshot| !audit_observed_is_tradeable(snapshot))
        .map(|_| "bootstrap_in_progress")
        .or_else(|| observed.is_none().then_some("bootstrap_in_progress"))
}

fn open_market_pool_gate_decision(
    settings: &Settings,
    observed: &mut ObservedState,
    observed_snapshot: Option<&AuditObservedMarket>,
    retention_reason: MarketRetentionReason,
    tradeable_gate_enabled: bool,
    now_ms: u64,
) -> Option<(String, Option<u64>)> {
    clear_market_pool_cooldown_if_needed(observed, now_ms);

    if let Some(reason) = market_pool_cooldown_reason(observed, now_ms) {
        return Some((
            cooldown_gate_reason(reason),
            market_pool_cooldown_remaining_ms(observed, now_ms),
        ));
    }

    if retention_reason != MarketRetentionReason::None {
        return Some((format!("retained_{}", retention_reason.as_str()), None));
    }

    if !tradeable_gate_enabled {
        return None;
    }

    let tradeable_now = observed_snapshot.is_some_and(audit_observed_is_tradeable);
    let market_tier = determine_market_tier(
        settings,
        tradeable_now,
        retention_reason,
        true,
        observed.market_pool.last_focus_at_ms,
        observed.market_pool.last_tradeable_at_ms,
        false,
        now_ms,
    );
    (market_tier != MarketTier::Tradeable).then(|| {
        (
            market_pool_gate_reason(observed, retention_reason, tradeable_now, now_ms),
            None,
        )
    })
}

fn market_pool_rejection_summary(
    candidate: &OpenCandidate,
    reason: &str,
    cooldown_remaining_ms: Option<u64>,
) -> String {
    match cooldown_remaining_ms {
        Some(remaining_ms) => format!(
            "候选被拒绝：{} {}（剩余冷却 {}）",
            gate_reason_label_zh("market_pool", reason),
            candidate.candidate_id,
            format_detail_ms(remaining_ms)
        ),
        None => format!(
            "候选被拒绝：{} {}",
            gate_reason_label_zh("market_pool", reason),
            candidate.candidate_id
        ),
    }
}

async fn process_signal_single(
    candidate: OpenCandidate,
    settings: &Settings,
    registry: &SymbolRegistry,
    observed: &mut ObservedState,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    trade_book: &mut TradeBook,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    ws_mode: bool,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    let fallback_token_side = Some(candidate.token_side);
    note_focus_market_activity(observed, candidate.timestamp_ms);
    let observed_snapshot = audit_snapshot_for_candidate(settings, registry, &candidate, observed);
    stats.signals_seen += 1;
    push_signal_event(
        recent_events,
        "signal",
        summarize_candidate(&candidate),
        &candidate,
        build_signal_event_details(&candidate, observed_snapshot.as_ref()),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_signal_emitted(&candidate, observed_snapshot.clone())?;
    }

    if emergency.load(Ordering::SeqCst) {
        let summary = format!("候选被拒绝：处于紧急停止状态 {}", candidate.candidate_id);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "emergency_stop",
                AuditGateResult::Rejected,
                "emergency_stop",
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "emergency",
            "emergency_stop",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "emergency_stop",
                AuditGateResult::Rejected,
                "emergency_stop",
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if paused.load(Ordering::SeqCst) {
        let summary = format!("候选被拒绝：交易已暂停 {}", candidate.candidate_id);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "trading_pause",
                AuditGateResult::Rejected,
                "trading_paused",
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "paused",
            "trading_paused",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "trading_pause",
                AuditGateResult::Rejected,
                "trading_paused",
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if ws_mode {
        if let Some(reason) = observed_snapshot.as_ref().and_then(ws_signal_gate_reason) {
            let _ = maybe_start_market_pool_cooldown(
                observed,
                settings,
                candidate.timestamp_ms,
                reason,
            );
            let summary = format!(
                "候选被拒绝：WS 行情新鲜度检查未通过 {}（{}）",
                candidate.candidate_id,
                format_ws_freshness_reason(&reason)
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    &reason,
                    summary.clone(),
                    observed_snapshot.clone(),
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "stale",
                reason,
                summary,
                build_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    &reason,
                    None,
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        } else if observed_snapshot.is_none() {
            let _ = maybe_start_market_pool_cooldown(
                observed,
                settings,
                candidate.timestamp_ms,
                "missing_observed_state",
            );
            let summary = format!(
                "候选被拒绝：WS 行情新鲜度检查未通过 {}（{}）",
                candidate.candidate_id,
                format_ws_freshness_reason("missing_observed_state")
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    "missing_observed_state",
                    summary.clone(),
                    None,
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "stale",
                "missing_observed_state",
                summary,
                build_gate_event_details(
                    &candidate,
                    None,
                    settings,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    "missing_observed_state",
                    None,
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        } else {
            let summary = format!("候选通过 WS 行情新鲜度检查 {}", candidate.candidate_id);
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Passed,
                    "ok",
                    summary.clone(),
                    observed_snapshot.clone(),
                )?;
            }
            push_gate_event(
                recent_events,
                &candidate,
                summary,
                build_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    "ws_freshness",
                    AuditGateResult::Passed,
                    "ok",
                    None,
                ),
            );
        }
    }

    let retention_reason = market_retention_reason(
        risk.position_tracker()
            .symbol_has_open_position(&candidate.symbol),
        execution.active_plan_priority(&candidate.symbol),
    );
    if let Some((reason_code, cooldown_remaining_ms)) = open_market_pool_gate_decision(
        settings,
        observed,
        observed_snapshot.as_ref(),
        retention_reason,
        ws_mode,
        candidate.timestamp_ms,
    ) {
        let summary =
            market_pool_rejection_summary(&candidate, &reason_code, cooldown_remaining_ms);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "market_pool",
                AuditGateResult::Rejected,
                &reason_code,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "risk",
            &reason_code,
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "market_pool",
                AuditGateResult::Rejected,
                &reason_code,
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if let Some(filter_reason) =
        candidate_price_filter_rejection_reason(&candidate, observed, settings)
    {
        let _ = maybe_start_market_pool_cooldown(
            observed,
            settings,
            candidate.timestamp_ms,
            filter_reason,
        );
        let filter_price = observed_snapshot
            .as_ref()
            .and_then(|observed| candidate_price_for_audit_observed(&candidate, observed))
            .map(format_detail_f64)
            .unwrap_or_else(|| "无可用价格".to_owned());
        let summary = format!(
            "候选被拒绝：{} {}（价格 {}，区间 {}）",
            gate_reason_label_zh("price_filter", filter_reason),
            candidate.candidate_id,
            filter_price,
            price_filter_window_text_for_symbol(settings, &candidate.symbol)
        );
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "price_filter",
                AuditGateResult::Rejected,
                filter_reason,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "price_filter",
            filter_reason,
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "price_filter",
                AuditGateResult::Rejected,
                filter_reason,
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }
    let (price_filter_result, price_filter_reason, price_filter_summary) = (
        AuditGateResult::Passed,
        "ok",
        format!("候选通过价格过滤 {}", candidate.candidate_id),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_gate_decision(
            &candidate,
            "price_filter",
            price_filter_result,
            price_filter_reason,
            price_filter_summary.clone(),
            observed_snapshot.clone(),
        )?;
    }
    push_gate_event(
        recent_events,
        &candidate,
        price_filter_summary,
        build_gate_event_details(
            &candidate,
            observed_snapshot.as_ref(),
            settings,
            "price_filter",
            price_filter_result,
            price_filter_reason,
            None,
        ),
    );

    let plan = match preview_open_plan_detailed(execution, risk, &candidate) {
        Ok(plan) => plan,
        Err(err) => {
            stats.signals_rejected += 1;
            stats.execution_rejected += 1;
            record_signal_rejection(stats, "execution_planner");
            let (reason_code, planning_diagnostics) = match err {
                PreviewIntentError::PlanRejected(rejection) => {
                    (rejection.reason.code().to_owned(), rejection.diagnostics)
                }
                PreviewIntentError::Core(err) => (
                    extract_plan_rejection_reason(&err.to_string())
                        .unwrap_or_else(|| "trade_plan_rejected".to_owned()),
                    None,
                ),
            };
            let _ = maybe_start_market_pool_cooldown(
                observed,
                settings,
                candidate.timestamp_ms,
                &reason_code,
            );
            let summary = format!(
                "候选被拒绝：未通过交易规划 {}（{}）",
                candidate.candidate_id,
                gate_reason_label_zh("trade_plan", &reason_code)
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision_with_diagnostics(
                    &candidate,
                    "trade_plan",
                    AuditGateResult::Rejected,
                    &reason_code,
                    summary.clone(),
                    observed_snapshot.clone(),
                    planning_diagnostics.clone(),
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "risk",
                &reason_code,
                summary,
                build_trade_plan_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    AuditGateResult::Rejected,
                    &reason_code,
                    planning_diagnostics.as_ref(),
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        }
    };

    if let Some(risk_rejection) = open_plan_risk_rejection_reason(risk, &plan) {
        let summary = format!(
            "候选被拒绝：未通过风控检查 {}（{}）",
            candidate.candidate_id,
            translate_risk_rejection(&risk_rejection)
        );
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "risk_guard",
                AuditGateResult::Rejected,
                &risk_rejection,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "risk",
            "risk_guard",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "risk_guard",
                AuditGateResult::Rejected,
                &risk_rejection,
                Some(risk),
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }
    let risk_pass_summary = format!("候选通过风控检查 {}", candidate.candidate_id);
    if let Some(audit) = audit.as_mut() {
        audit.record_gate_decision(
            &candidate,
            "risk_guard",
            AuditGateResult::Passed,
            "ok",
            risk_pass_summary.clone(),
            observed_snapshot.clone(),
        )?;
    }
    push_gate_event(
        recent_events,
        &candidate,
        risk_pass_summary,
        build_gate_event_details(
            &candidate,
            observed_snapshot.as_ref(),
            settings,
            "risk_guard",
            AuditGateResult::Passed,
            "ok",
            Some(risk),
        ),
    );

    register_open_trade_from_candidate(trade_book, &candidate, engine);
    let trigger = RuntimeTrigger::Candidate(candidate.clone());
    let events = match execution.process_plan(plan).await {
        Ok(events) => events,
        Err(err) => {
            stats.signals_rejected += 1;
            stats.execution_rejected += 1;
            record_signal_rejection(stats, "execution_planner");
            trade_book.discard_open_trade(&candidate.symbol);
            let rejection_reasons = vec![err.to_string()];
            let _ = maybe_start_market_pool_cooldown_from_rejections(
                observed,
                settings,
                candidate.timestamp_ms,
                &rejection_reasons,
            );
            push_event_with_context_and_payload(
                recent_events,
                "risk",
                format!(
                    "候选被执行规划拒绝 {}（{}）",
                    candidate.candidate_id,
                    rejection_reasons.join(", ")
                ),
                Some(candidate.symbol.0.clone()),
                Some(candidate.candidate_id.clone()),
                Some(candidate.correlation_id.clone()),
                build_post_submit_rejection_details(&candidate, &rejection_reasons),
                Some(post_submit_rejection_payload(
                    &candidate,
                    &rejection_reasons,
                )),
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_open_signal_rejected_post_submit(&candidate, &rejection_reasons)?;
            }
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        }
    };
    if execution_events_include_trade_plan_created(&events) {
        note_tradeable_market_activity(observed, candidate.timestamp_ms);
    }
    let rejection_reasons = opening_signal_rejection_reasons(&events);
    if !rejection_reasons.is_empty() {
        let _ = maybe_start_market_pool_cooldown_from_rejections(
            observed,
            settings,
            candidate.timestamp_ms,
            &rejection_reasons,
        );
    }
    track_open_signal_execution_outcome(
        stats,
        recent_events,
        trade_book,
        &candidate,
        &events,
        audit,
    )?;
    apply_execution_events(
        risk,
        stats,
        recent_events,
        trade_book,
        Some(&trigger),
        events,
        audit,
    )
    .await?;
    sync_engine_position_state_from_runtime(
        engine,
        risk,
        trade_book,
        &candidate.symbol,
        fallback_token_side,
    );
    Ok(())
}

async fn process_signal_multi(
    candidate: OpenCandidate,
    settings: &Settings,
    registry: &SymbolRegistry,
    observed_map: &mut HashMap<Symbol, ObservedState>,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    trade_book: &mut TradeBook,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    ws_mode: bool,
    bootstrap_in_flight: bool,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    let fallback_token_side = Some(candidate.token_side);
    if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
        note_focus_market_activity(observed, candidate.timestamp_ms);
    }
    let observed_snapshot = observed_map.get(&candidate.symbol).and_then(|observed| {
        audit_snapshot_for_candidate(settings, registry, &candidate, observed)
    });
    stats.signals_seen += 1;
    push_signal_event(
        recent_events,
        "signal",
        summarize_candidate(&candidate),
        &candidate,
        build_signal_event_details(&candidate, observed_snapshot.as_ref()),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_signal_emitted(&candidate, observed_snapshot.clone())?;
    }

    if emergency.load(Ordering::SeqCst) {
        let summary = format!("候选被拒绝：处于紧急停止状态 {}", candidate.candidate_id);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "emergency_stop",
                AuditGateResult::Rejected,
                "emergency_stop",
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "emergency",
            "emergency_stop",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "emergency_stop",
                AuditGateResult::Rejected,
                "emergency_stop",
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if paused.load(Ordering::SeqCst) {
        let summary = format!("候选被拒绝：交易已暂停 {}", candidate.candidate_id);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "trading_pause",
                AuditGateResult::Rejected,
                "trading_paused",
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "paused",
            "trading_paused",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "trading_pause",
                AuditGateResult::Rejected,
                "trading_paused",
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if ws_mode {
        if let Some(reason) =
            ws_bootstrap_gate_reason(bootstrap_in_flight, observed_snapshot.as_ref())
        {
            let summary = format!(
                "候选被拒绝：WS 行情启动补齐尚未完成 {}（{}）",
                candidate.candidate_id,
                format_ws_freshness_reason(reason)
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    reason,
                    summary.clone(),
                    observed_snapshot.clone(),
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "stale",
                reason,
                summary,
                build_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    reason,
                    None,
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        } else if let Some(reason) = observed_snapshot.as_ref().and_then(ws_signal_gate_reason) {
            if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
                let _ = maybe_start_market_pool_cooldown(
                    observed,
                    settings,
                    candidate.timestamp_ms,
                    reason,
                );
            }
            let summary = format!(
                "候选被拒绝：WS 行情新鲜度检查未通过 {}（{}）",
                candidate.candidate_id,
                format_ws_freshness_reason(reason)
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    reason,
                    summary.clone(),
                    observed_snapshot.clone(),
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "stale",
                reason,
                summary,
                build_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    reason,
                    None,
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        } else if observed_snapshot.is_none() {
            if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
                let _ = maybe_start_market_pool_cooldown(
                    observed,
                    settings,
                    candidate.timestamp_ms,
                    "missing_observed_state",
                );
            }
            let summary = format!(
                "候选被拒绝：WS 行情新鲜度检查未通过 {}（{}）",
                candidate.candidate_id,
                format_ws_freshness_reason("missing_observed_state")
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    "missing_observed_state",
                    summary.clone(),
                    None,
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "stale",
                "missing_observed_state",
                summary,
                build_gate_event_details(
                    &candidate,
                    None,
                    settings,
                    "ws_freshness",
                    AuditGateResult::Rejected,
                    "missing_observed_state",
                    None,
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        } else {
            let summary = format!("候选通过 WS 行情新鲜度检查 {}", candidate.candidate_id);
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision(
                    &candidate,
                    "ws_freshness",
                    AuditGateResult::Passed,
                    "ok",
                    summary.clone(),
                    observed_snapshot.clone(),
                )?;
            }
            push_gate_event(
                recent_events,
                &candidate,
                summary,
                build_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    "ws_freshness",
                    AuditGateResult::Passed,
                    "ok",
                    None,
                ),
            );
        }
    }

    let retention_reason = market_retention_reason(
        risk.position_tracker()
            .symbol_has_open_position(&candidate.symbol),
        execution.active_plan_priority(&candidate.symbol),
    );
    if let Some((reason_code, cooldown_remaining_ms)) = observed_map
        .get_mut(&candidate.symbol)
        .and_then(|observed| {
            open_market_pool_gate_decision(
                settings,
                observed,
                observed_snapshot.as_ref(),
                retention_reason,
                ws_mode,
                candidate.timestamp_ms,
            )
        })
    {
        let summary =
            market_pool_rejection_summary(&candidate, &reason_code, cooldown_remaining_ms);
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "market_pool",
                AuditGateResult::Rejected,
                &reason_code,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "risk",
            &reason_code,
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "market_pool",
                AuditGateResult::Rejected,
                &reason_code,
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }

    if let Some(filter_reason) =
        candidate_price_filter_rejection_reason_multi(&candidate, observed_map, settings)
    {
        if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
            let _ = maybe_start_market_pool_cooldown(
                observed,
                settings,
                candidate.timestamp_ms,
                filter_reason,
            );
        }
        let filter_price = observed_snapshot
            .as_ref()
            .and_then(|observed| candidate_price_for_audit_observed(&candidate, observed))
            .map(format_detail_f64)
            .unwrap_or_else(|| "无可用价格".to_owned());
        let summary = format!(
            "候选被拒绝：{} {}（价格 {}，区间 {}）",
            gate_reason_label_zh("price_filter", filter_reason),
            candidate.candidate_id,
            filter_price,
            price_filter_window_text_for_symbol(settings, &candidate.symbol)
        );
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "price_filter",
                AuditGateResult::Rejected,
                filter_reason,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "price_filter",
            filter_reason,
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "price_filter",
                AuditGateResult::Rejected,
                filter_reason,
                None,
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }
    let (price_filter_result, price_filter_reason, price_filter_summary) = (
        AuditGateResult::Passed,
        "ok",
        format!("候选通过价格过滤 {}", candidate.candidate_id),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_gate_decision(
            &candidate,
            "price_filter",
            price_filter_result,
            price_filter_reason,
            price_filter_summary.clone(),
            observed_snapshot.clone(),
        )?;
    }
    push_gate_event(
        recent_events,
        &candidate,
        price_filter_summary,
        build_gate_event_details(
            &candidate,
            observed_snapshot.as_ref(),
            settings,
            "price_filter",
            price_filter_result,
            price_filter_reason,
            None,
        ),
    );

    let plan = match preview_open_plan_detailed(execution, risk, &candidate) {
        Ok(plan) => plan,
        Err(err) => {
            stats.signals_rejected += 1;
            stats.execution_rejected += 1;
            record_signal_rejection(stats, "execution_planner");
            let (reason_code, planning_diagnostics) = match err {
                PreviewIntentError::PlanRejected(rejection) => {
                    (rejection.reason.code().to_owned(), rejection.diagnostics)
                }
                PreviewIntentError::Core(err) => (
                    extract_plan_rejection_reason(&err.to_string())
                        .unwrap_or_else(|| "trade_plan_rejected".to_owned()),
                    None,
                ),
            };
            if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
                let _ = maybe_start_market_pool_cooldown(
                    observed,
                    settings,
                    candidate.timestamp_ms,
                    &reason_code,
                );
            }
            let summary = format!(
                "候选被拒绝：未通过交易规划 {}（{}）",
                candidate.candidate_id,
                gate_reason_label_zh("trade_plan", &reason_code)
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_gate_decision_with_diagnostics(
                    &candidate,
                    "trade_plan",
                    AuditGateResult::Rejected,
                    &reason_code,
                    summary.clone(),
                    observed_snapshot.clone(),
                    planning_diagnostics.clone(),
                )?;
            }
            reject_signal(
                stats,
                recent_events,
                &candidate,
                "risk",
                &reason_code,
                summary,
                build_trade_plan_gate_event_details(
                    &candidate,
                    observed_snapshot.as_ref(),
                    settings,
                    AuditGateResult::Rejected,
                    &reason_code,
                    planning_diagnostics.as_ref(),
                ),
            );
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        }
    };

    if let Some(risk_rejection) = open_plan_risk_rejection_reason(risk, &plan) {
        let summary = format!(
            "候选被拒绝：未通过风控检查 {}（{}）",
            candidate.candidate_id,
            translate_risk_rejection(&risk_rejection)
        );
        if let Some(audit) = audit.as_mut() {
            audit.record_gate_decision(
                &candidate,
                "risk_guard",
                AuditGateResult::Rejected,
                &risk_rejection,
                summary.clone(),
                observed_snapshot.clone(),
            )?;
        }
        reject_signal(
            stats,
            recent_events,
            &candidate,
            "risk",
            "risk_guard",
            summary,
            build_gate_event_details(
                &candidate,
                observed_snapshot.as_ref(),
                settings,
                "risk_guard",
                AuditGateResult::Rejected,
                &risk_rejection,
                Some(risk),
            ),
        );
        sync_engine_position_state_from_runtime(
            engine,
            risk,
            trade_book,
            &candidate.symbol,
            fallback_token_side,
        );
        return Ok(());
    }
    let risk_pass_summary = format!("候选通过风控检查 {}", candidate.candidate_id);
    if let Some(audit) = audit.as_mut() {
        audit.record_gate_decision(
            &candidate,
            "risk_guard",
            AuditGateResult::Passed,
            "ok",
            risk_pass_summary.clone(),
            observed_snapshot.clone(),
        )?;
    }
    push_gate_event(
        recent_events,
        &candidate,
        risk_pass_summary,
        build_gate_event_details(
            &candidate,
            observed_snapshot.as_ref(),
            settings,
            "risk_guard",
            AuditGateResult::Passed,
            "ok",
            Some(risk),
        ),
    );

    register_open_trade_from_candidate(trade_book, &candidate, engine);
    let trigger = RuntimeTrigger::Candidate(candidate.clone());
    let events = match execution.process_plan(plan).await {
        Ok(events) => events,
        Err(err) => {
            stats.signals_rejected += 1;
            stats.execution_rejected += 1;
            record_signal_rejection(stats, "execution_planner");
            trade_book.discard_open_trade(&candidate.symbol);
            let rejection_reasons = vec![err.to_string()];
            if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
                let _ = maybe_start_market_pool_cooldown_from_rejections(
                    observed,
                    settings,
                    candidate.timestamp_ms,
                    &rejection_reasons,
                );
            }
            push_event_with_context_and_payload(
                recent_events,
                "risk",
                format!(
                    "候选被执行规划拒绝 {}（{}）",
                    candidate.candidate_id,
                    rejection_reasons.join(", ")
                ),
                Some(candidate.symbol.0.clone()),
                Some(candidate.candidate_id.clone()),
                Some(candidate.correlation_id.clone()),
                build_post_submit_rejection_details(&candidate, &rejection_reasons),
                Some(post_submit_rejection_payload(
                    &candidate,
                    &rejection_reasons,
                )),
            );
            if let Some(audit) = audit.as_mut() {
                audit.record_open_signal_rejected_post_submit(&candidate, &rejection_reasons)?;
            }
            sync_engine_position_state_from_runtime(
                engine,
                risk,
                trade_book,
                &candidate.symbol,
                fallback_token_side,
            );
            return Ok(());
        }
    };
    if execution_events_include_trade_plan_created(&events) {
        if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
            note_tradeable_market_activity(observed, candidate.timestamp_ms);
        }
    }
    let rejection_reasons = opening_signal_rejection_reasons(&events);
    if !rejection_reasons.is_empty() {
        if let Some(observed) = observed_map.get_mut(&candidate.symbol) {
            let _ = maybe_start_market_pool_cooldown_from_rejections(
                observed,
                settings,
                candidate.timestamp_ms,
                &rejection_reasons,
            );
        }
    }
    track_open_signal_execution_outcome(
        stats,
        recent_events,
        trade_book,
        &candidate,
        &events,
        audit,
    )?;
    apply_execution_events(
        risk,
        stats,
        recent_events,
        trade_book,
        Some(&trigger),
        events,
        audit,
    )
    .await?;
    sync_engine_position_state_from_runtime(
        engine,
        risk,
        trade_book,
        &candidate.symbol,
        fallback_token_side,
    );
    Ok(())
}

async fn handle_single_market_event(
    event: MarketDataEvent,
    market: &MarketConfig,
    settings: &Settings,
    registry: &SymbolRegistry,
    orderbook_provider: &Arc<InMemoryOrderbookProvider>,
    observed: &mut ObservedState,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    trade_book: &mut TradeBook,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    stats.market_events += 1;
    update_paper_orderbook_for_event(orderbook_provider, registry, &event);
    let previous_market_phase = match &event {
        MarketDataEvent::MarketLifecycle { .. } => observed.market_phase.clone(),
        _ => None,
    };
    let touches_market = match &event {
        MarketDataEvent::ConnectionEvent { .. } => true,
        _ => market_symbols_for_event(&event, registry)
            .iter()
            .any(|symbol| symbol == &market.symbol),
    };
    if touches_market {
        observe_market_event(observed, &event);
    }
    match &event {
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            let exchange_name = format!("{exchange:?}");
            let connections = aggregate_connections(
                &HashMap::from([(market.symbol.clone(), observed.clone())]),
                now_millis(),
            );
            push_event_with_context(
                recent_events,
                "connection",
                summarize_market_event(&event),
                Some(market.symbol.0.clone()),
                None,
                None,
                build_connection_event_details(
                    &exchange_name,
                    *status,
                    connections.get(&exchange_name),
                    1,
                ),
            );
        }
        MarketDataEvent::MarketLifecycle {
            symbol,
            phase,
            timestamp_ms,
        } => {
            if previous_market_phase.as_ref() != Some(phase) {
                push_event_with_context(
                    recent_events,
                    "market_phase",
                    summarize_market_event(&event),
                    Some(symbol.0.clone()),
                    None,
                    None,
                    build_market_phase_event_details(symbol, phase, *timestamp_ms),
                );
            }
        }
        _ => push_event_simple(recent_events, "market_data", summarize_market_event(&event)),
    }

    if let MarketDataEvent::MarketLifecycle { symbol, phase, .. } = &event {
        risk.update_market_phase(symbol.clone(), phase.clone());
    }

    let output = engine.on_market_data(&event).await;
    if touches_market {
        let basis = effective_basis_config_for_market(settings, market);
        let evaluation_observed = build_audit_observed_market(
            now_millis(),
            market,
            observed,
            settings,
            None,
            false,
            &basis,
        );
        record_evaluation_attempt(
            stats,
            recent_events,
            &event,
            &output.warnings,
            Some(&evaluation_observed),
            audit,
        )?;
    }
    for update in output.dmm_updates {
        let events = execution.apply_dmm_quote_update(update).await?;
        apply_execution_events(risk, stats, recent_events, trade_book, None, events, audit).await?;
    }

    for candidate in output.open_candidates {
        process_signal_single(
            candidate,
            settings,
            registry,
            observed,
            stats,
            recent_events,
            engine,
            execution,
            risk,
            trade_book,
            paused,
            emergency,
            matches!(settings.strategy.market_data.mode, MarketDataMode::Ws),
            audit,
        )
        .await?;
    }

    if let Some(reason) = engine.close_reason(&market.symbol) {
        let now_ms = now_millis();
        execute_intent_trigger(
            close_intent_for_symbol(
                &market.symbol,
                &reason,
                &format!("corr-close-{}-{now_ms}", market.symbol.0),
                now_ms,
            ),
            now_ms,
            engine,
            execution,
            risk,
            stats,
            recent_events,
            trade_book,
            audit,
        )
        .await?;
    } else {
        maybe_execute_delta_rebalance(
            settings,
            &HashMap::from([(market.symbol.clone(), observed.clone())]),
            &market.symbol,
            engine,
            execution,
            risk,
            stats,
            recent_events,
            trade_book,
            audit,
            now_millis(),
        )
        .await?;
    }

    let _ = market;
    Ok(())
}

async fn handle_multi_market_event(
    event: MarketDataEvent,
    settings: &Settings,
    registry: &SymbolRegistry,
    orderbook_provider: &Arc<InMemoryOrderbookProvider>,
    observed: &mut HashMap<Symbol, ObservedState>,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<RuntimeExecutor>,
    risk: &mut InMemoryRiskManager,
    trade_book: &mut TradeBook,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    bootstrap_in_flight: bool,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    stats.market_events += 1;
    let event_symbols = market_symbols_for_event(&event, registry);
    let previous_market_phase = match &event {
        MarketDataEvent::MarketLifecycle { symbol, .. } => observed
            .get(symbol)
            .and_then(|obs| obs.market_phase.clone()),
        _ => None,
    };
    let previous_connection_status = match &event {
        MarketDataEvent::ConnectionEvent { exchange, .. } => observed
            .values()
            .find_map(|obs| obs.connections.get(&format!("{exchange:?}")).copied()),
        _ => None,
    };

    update_paper_orderbook_for_event(orderbook_provider, registry, &event);

    if let MarketDataEvent::ConnectionEvent { exchange, status } = &event {
        for obs in observed.values_mut() {
            record_connection_status(obs, format!("{exchange:?}"), *status, now_millis());
        }
    } else {
        for symbol in &event_symbols {
            if let Some(obs) = observed.get_mut(symbol) {
                observe_market_event(obs, &event);
            }
        }
    }

    match &event {
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            if previous_connection_status != Some(*status) {
                let exchange_name = format!("{exchange:?}");
                let connections = aggregate_connections(observed, now_millis());
                push_event_with_context(
                    recent_events,
                    "connection",
                    summarize_market_event(&event),
                    None,
                    None,
                    None,
                    build_connection_event_details(
                        &exchange_name,
                        *status,
                        connections.get(&exchange_name),
                        observed.len(),
                    ),
                );
            }
        }
        MarketDataEvent::MarketLifecycle {
            symbol,
            phase,
            timestamp_ms,
        } => {
            if previous_market_phase.as_ref() != Some(phase) {
                push_event_with_context(
                    recent_events,
                    "market_phase",
                    summarize_market_event(&event),
                    Some(symbol.0.clone()),
                    None,
                    None,
                    build_market_phase_event_details(symbol, phase, *timestamp_ms),
                );
            }
        }
        _ => {}
    }

    if let MarketDataEvent::MarketLifecycle { symbol, phase, .. } = &event {
        risk.update_market_phase(symbol.clone(), phase.clone());
    }

    let evaluation_observed_by_symbol = event_symbols
        .iter()
        .filter_map(|symbol| {
            observed
                .get(symbol)
                .and_then(|obs| registry.get_config(symbol).map(|market| (obs, market)))
                .map(|(obs, market)| {
                    let basis = effective_basis_config_for_market(settings, market);
                    (
                        symbol.clone(),
                        build_audit_observed_market(
                            now_millis(),
                            market,
                            obs,
                            settings,
                            None,
                            false,
                            &basis,
                        ),
                    )
                })
        })
        .collect::<HashMap<_, _>>();

    let output = engine.on_market_data(&event).await;
    record_evaluation_attempts_for_symbols(
        stats,
        recent_events,
        &event,
        &event_symbols,
        &output.warnings,
        &evaluation_observed_by_symbol,
        audit,
    )?;
    for update in output.dmm_updates {
        let events = execution.apply_dmm_quote_update(update).await?;
        apply_execution_events(risk, stats, recent_events, trade_book, None, events, audit).await?;
    }

    for candidate in output.open_candidates {
        process_signal_multi(
            candidate,
            settings,
            registry,
            observed,
            stats,
            recent_events,
            engine,
            execution,
            risk,
            trade_book,
            paused,
            emergency,
            matches!(settings.strategy.market_data.mode, MarketDataMode::Ws),
            bootstrap_in_flight,
            audit,
        )
        .await?;
    }

    for symbol in &event_symbols {
        if let Some(reason) = engine.close_reason(symbol) {
            let now_ms = now_millis();
            execute_intent_trigger(
                close_intent_for_symbol(
                    symbol,
                    &reason,
                    &format!("corr-close-{}-{now_ms}", symbol.0),
                    now_ms,
                ),
                now_ms,
                engine,
                execution,
                risk,
                stats,
                recent_events,
                trade_book,
                audit,
            )
            .await?;
        } else {
            maybe_execute_delta_rebalance(
                settings,
                observed,
                symbol,
                engine,
                execution,
                risk,
                stats,
                recent_events,
                trade_book,
                audit,
                now_millis(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_paper_ws_mode(
    env: &str,
    mut settings: Settings,
    market_index: usize,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    execution_mode: RuntimeExecutionMode,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<()> {
    let market = select_market(&settings, market_index)?;
    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());

    let socket_path = monitor_socket_path(&settings);
    let current_state: Arc<std::sync::RwLock<Option<MonitorState>>> =
        Arc::new(std::sync::RwLock::new(None));
    let paused = Arc::new(AtomicBool::new(false));
    let emergency = Arc::new(AtomicBool::new(false));
    let pre_emergency_paused = Arc::new(AtomicBool::new(false));
    let command_center = Arc::new(Mutex::new(CommandCenter::default()));

    let socket_server = MonitorSocketServer::new_with_current_state(
        socket_path.clone(),
        Arc::clone(&current_state),
    );
    let monitor_client_count = socket_server.client_count_handle();
    let state_broadcaster = socket_server.state_broadcaster();
    let event_broadcaster = socket_server.event_broadcaster();
    let command_center_clone = Arc::clone(&command_center);
    tokio::spawn(async move {
        let handler = move |command_id: String, kind: CommandKind| {
            let now_ms = now_millis();
            let mut center = command_center_clone
                .lock()
                .map_err(|_| "命令中心锁异常".to_owned())?;
            let command = match kind {
                CommandKind::CancelCommand { command_id: target } => {
                    center.cancel(command_id, target, now_ms)
                }
                other => center.enqueue(command_id, other, now_ms),
            };
            Ok(CommandCenter::ack_message(&command))
        };
        if let Err(e) = socket_server.run(Box::new(handler)).await {
            tracing::error!("监控 Socket 服务异常: {}", e);
        }
    });

    let poly_source = PolymarketLiveDataSource::new(
        manager.clone(),
        settings.polymarket.clob_api_url.clone(),
        settings.strategy.settlement.clone(),
    );
    let cex_source = build_cex_source(manager.clone(), &market, &settings);

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = build_paper_engine_config(&settings);
    let market_overrides = build_market_overrides(&settings, &settings.markets);

    let entry_z = market_overrides
        .get(&market.symbol)
        .and_then(|o| o.entry_z)
        .unwrap_or(engine_config.entry_z);
    println!(
        "参数：{} ({}) 入场Z = {}（默认：{}）[行情=WS]",
        market.symbol.0, market.cex_symbol, entry_z, engine_config.entry_z
    );

    let mut engine =
        SimpleAlphaEngine::with_markets(engine_config.clone(), settings.markets.clone());
    engine.set_market_overrides(market_overrides);
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor_start_ms = now_millis();
    let (orderbook_provider, executor, mut execution) = build_paper_execution_stack(
        &settings,
        &registry,
        executor_start_ms,
        execution_mode,
        Some(&market),
    )
    .await?;
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed = ObservedState::default();
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut last_event_seq_sent = 0_u64;
    let mut snapshots = Vec::new();
    let mut trade_book = TradeBook::default();
    let start_time_ms = executor_start_ms;
    let mut audit = if settings.audit.enabled {
        let audit = PaperAudit::start(
            &settings,
            env,
            std::slice::from_ref(&market),
            executor_start_ms,
            trading_mode,
        )?;
        println!(
            "审计会话已启动：{} -> {}",
            audit.session_id(),
            audit.session_dir().display()
        );
        Some(audit)
    } else {
        None
    };

    if warmup_klines > 0 {
        if let Err(err) = warmup_market_engine(
            &market,
            &mut engine,
            &cex_source,
            &poly_source,
            warmup_klines,
        )
        .await
        {
            push_event_simple(
                &mut recent_events,
                "warmup",
                format!("{} 预热失败：{}", market.symbol.0, err),
            );
        }
    }

    bootstrap_single_market_snapshots(
        &market,
        &poly_source,
        &cex_source,
        depth,
        include_funding,
        &mut recent_events,
        &mut stats,
    )
    .await;

    let monitor_market_cache: Arc<std::sync::RwLock<HashMap<Symbol, MonitorMarketSeed>>> =
        Arc::new(std::sync::RwLock::new(HashMap::new()));
    refresh_single_monitor_cache(
        &monitor_market_cache,
        &market.symbol,
        &observed,
        &engine,
        &execution,
        &risk,
    );
    let (monitor_render_tx, mut monitor_render_rx) =
        mpsc::channel::<MultiMonitorRenderRequest>(1);
    let (monitor_render_result_tx, mut monitor_render_result_rx) =
        mpsc::channel::<MultiMonitorRenderResult>(1);
    let monitor_market_cache_clone = Arc::clone(&monitor_market_cache);
    let mut monitor_render_task = Some(tokio::spawn(async move {
        while let Some(request) = monitor_render_rx.recv().await {
            let result = {
                let cache = monitor_market_cache_clone
                    .read()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                build_multi_monitor_state_from_seed_cache(&request, &cache)
            };
            if monitor_render_result_tx.send(result).await.is_err() {
                break;
            }
        }
    }));
    let mut monitor_render_in_flight = false;

    let mut ws_tasks = spawn_ws_market_data_tasks(
        manager.clone(),
        &settings,
        std::slice::from_ref(&market),
        depth,
        stats.polymarket_ws_diagnostics.clone(),
    );
    let funding_targets = vec![funding_target_for_market(&market)];
    let (funding_refresh_tx, mut funding_refresh_rx) =
        mpsc::unbounded_channel::<FundingRefreshResult>();
    let mut funding_refresh_task: Option<JoinHandle<()>> = None;

    let mut housekeeping = ws_housekeeping_interval(poll_interval_ms);
    housekeeping.tick().await;
    let mut snapshot_refresh = interval(Duration::from_secs(
        settings.strategy.market_data.snapshot_refresh_secs.max(1),
    ));
    snapshot_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    snapshot_refresh.tick().await;
    let mut monitor_publish = interval(Duration::from_millis(MONITOR_PUBLISH_THROTTLE_MS));
    monitor_publish.set_missed_tick_behavior(MissedTickBehavior::Skip);
    monitor_publish.tick().await;
    let mut funding_poll = interval(Duration::from_secs(
        settings.strategy.market_data.funding_poll_secs.max(1),
    ));
    funding_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    funding_poll.tick().await;
    let mut state_dirty = true;
    let mut market_data_events_since_last_tick = 0_u64;

    let loop_result: Result<()> = async {
        loop {
            tokio::select! {
            Some(rendered) = monitor_render_result_rx.recv() => {
                monitor_render_in_flight = false;
                if let Some(audit) = audit.as_mut() {
                    audit.maybe_record_runtime_state(
                        &rendered.stats,
                        &rendered.monitor_state,
                        rendered.tick_index,
                        rendered.now_ms,
                    )?;
                }
                if rendered.has_monitor_clients {
                    if let Ok(mut guard) = current_state.write() {
                        *guard = Some(rendered.monitor_state.clone());
                    }
                    let _ = state_broadcaster.send(rendered.monitor_state);
                } else if let Ok(mut guard) = current_state.write() {
                    *guard = Some(rendered.monitor_state);
                }
            }
            Some(result) = funding_refresh_rx.recv() => {
                funding_refresh_task = None;
                handle_funding_refresh_result(&mut stats, &mut recent_events, result);
                state_dirty = true;
            }
            event = market_data_rx.recv() => {
                match event {
                    Ok(event) => {
                        market_data_events_since_last_tick =
                            market_data_events_since_last_tick.saturating_add(1);
                        handle_single_market_event(
                            event,
                            &market,
                            &settings,
                            &registry,
                            &orderbook_provider,
                            &mut observed,
                            &mut stats,
                            &mut recent_events,
                            &mut engine,
                            &mut execution,
                            &mut risk,
                            &mut trade_book,
                            &paused,
                            &emergency,
                            &mut audit,
                        ).await?;
                        refresh_single_monitor_cache(
                            &monitor_market_cache,
                            &market.symbol,
                            &observed,
                            &engine,
                            &execution,
                            &risk,
                        );
                        state_dirty = true;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged_messages)) => {
                        record_market_data_rx_lag(&mut stats, lagged_messages as u64);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = housekeeping.tick() => {
                record_market_data_tick_drain(
                    &mut stats,
                    &mut market_data_events_since_last_tick,
                );
                stats.ticks_processed += 1;
                let tick_index = stats.ticks_processed;
                let now_ms = now_millis();
                let now_secs = now_ms / 1000;

                drain_command_events(&command_center, &mut recent_events);
                let _ = process_pending_command_single(
                    &command_center,
                    &paused,
                    &emergency,
                    &pre_emergency_paused,
                    &mut settings,
                    &market,
                    &mut engine,
                    &mut execution,
                    &mut risk,
                    &mut stats,
                    &mut recent_events,
                    &mut trade_book,
                    &mut audit,
                    now_ms,
                ).await?;
                drain_command_events(&command_center, &mut recent_events);

                if next_market_lifecycle_phase(
                    Some(&observed),
                    &market,
                    now_secs,
                    &settings.strategy.settlement,
                )
                .is_some()
                {
                    if let Err(err) = manager.publish_market_lifecycle(
                        &market.symbol,
                        now_secs,
                        now_ms,
                        &settings.strategy.settlement,
                    ) {
                        stats.poll_errors += 1;
                        push_event_simple(
                            &mut recent_events,
                            "poll_error",
                            format!("市场生命周期发布失败：{err}"),
                        );
                    }
                } else {
                    observed.market_phase = Some(current_market_lifecycle_phase(
                        &market,
                        now_secs,
                        &settings.strategy.settlement,
                    ));
                }
                refresh_single_monitor_cache(
                    &monitor_market_cache,
                    &market.symbol,
                    &observed,
                    &engine,
                    &execution,
                    &risk,
                );

                let snapshot = build_snapshot(
                    &market,
                    tick_index,
                    &engine,
                    &observed,
                    &stats,
                    &risk,
                    &execution,
                    &executor,
                )?;
                if tick_index % print_every.max(1) == 0 {
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
                snapshots.push(snapshot.clone());
                state_dirty = true;

                if max_ticks > 0 && tick_index >= max_ticks {
                    break;
                }
            }
            _ = monitor_publish.tick(), if state_dirty => {
                let now_ms = now_millis();
                let tick_index = stats.ticks_processed;
                broadcast_monitor_events_since(
                    &recent_events,
                    &mut last_event_seq_sent,
                    &event_broadcaster,
                );
                let monitor_client_count = *monitor_client_count.read().await;
                let has_monitor_clients = monitor_client_count > 0;
                let should_render =
                    should_build_monitor_state(monitor_client_count, audit.as_ref(), now_ms);
                if !should_render {
                    state_dirty = false;
                } else if !monitor_render_in_flight {
                    let monitor_events = build_monitor_events(&recent_events);
                    let observed_map = HashMap::from([(market.symbol.clone(), observed.clone())]);
                    let request = build_monitor_render_request(
                        &settings,
                        &stats,
                        &observed_map,
                        &engine,
                        &risk,
                        &mut trade_book,
                        &command_center,
                        paused.load(Ordering::SeqCst),
                        emergency.load(Ordering::SeqCst),
                        start_time_ms,
                        now_ms,
                        &monitor_events,
                        trading_mode,
                        HashMap::new(),
                        0,
                        0,
                        0,
                        0,
                        has_monitor_clients,
                        false,
                        tick_index,
                        false,
                    );
                    if monitor_render_tx.try_send(request).is_ok() {
                        monitor_render_in_flight = true;
                        state_dirty = false;
                    }
                }
            }
            _ = snapshot_refresh.tick() => {
                stats.snapshot_resync_count += 1;
                bootstrap_single_market_snapshots(
                    &market,
                    &poly_source,
                    &cex_source,
                    depth,
                    false,
                    &mut recent_events,
                    &mut stats,
                ).await;
                state_dirty = true;
            }
            _ = funding_poll.tick(), if include_funding => {
                if funding_refresh_task.is_some() {
                    push_event_simple(
                        &mut recent_events,
                        "funding",
                        "上一轮后台资金费刷新仍在运行，本轮已跳过".to_owned(),
                    );
                    state_dirty = true;
                    continue;
                }

                stats.funding_refresh_count += 1;
                funding_refresh_task = Some(spawn_funding_refresh(
                    HashMap::from([(market.hedge_exchange, cex_source.clone())]),
                    funding_targets.clone(),
                    funding_refresh_tx.clone(),
                ));
                state_dirty = true;
            }
        }
        }
        Ok(())
    }
    .await;

    abort_join_handles(&mut ws_tasks).await;
    abort_optional_join_handle(&mut funding_refresh_task).await;
    abort_optional_join_handle(&mut monitor_render_task).await;

    let final_monitor_state = current_state
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| {
            let monitor_events = build_monitor_events(&recent_events);
            build_monitor_state(
                &settings,
                &market,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_millis(),
                &monitor_events,
                trading_mode,
            )
        });
    if let Ok(mut guard) = current_state.write() {
        *guard = Some(final_monitor_state.clone());
    }
    let runtime_result: Result<()> = async {
        loop_result?;
        let final_snapshot = snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("纸面盘会话未生成任何快照"))?;
        let open_orders = executor
            .open_order_snapshots()?
            .into_iter()
            .map(order_view_from_snapshot)
            .collect();
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode,
            env: env.to_owned(),
            market_index,
            market: market.symbol.0.clone(),
            hedge_exchange: format!("{:?}", market.hedge_exchange),
            market_data_mode: Some("ws".to_owned()),
            poll_interval_ms,
            depth,
            include_funding,
            ticks_processed: stats.ticks_processed,
            final_snapshot,
            open_orders,
            recent_events: recent_events.into_iter().collect(),
            snapshots,
        };
        let artifact_path = artifact_path(&settings, env, trading_mode)?;
        persist_artifact(&artifact_path, &artifact)?;
        println!(
            "{}结果已写入：{}",
            artifact_runtime_label(trading_mode),
            artifact_path.display()
        );
        Ok(())
    }
    .await;
    finish_runtime_with_audit(
        runtime_result,
        &mut audit,
        &stats,
        &final_monitor_state,
        stats.ticks_processed,
        now_millis(),
    )?;
    if let Some(audit) = audit.as_ref() {
        println!("审计会话已完成：{}", audit.session_dir().display());
    }

    Ok(())
}

async fn run_paper_multi_ws_mode(
    env: &str,
    mut settings: Settings,
    poll_interval_ms: u64,
    print_every: usize,
    max_ticks: usize,
    depth: u16,
    include_funding: bool,
    json: bool,
    warmup_klines: u16,
    execution_mode: RuntimeExecutionMode,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<()> {
    let markets = settings.markets.clone();
    if markets.is_empty() {
        anyhow::bail!("未配置任何市场");
    }

    let registry = SymbolRegistry::new(markets.clone());
    let symbols: Vec<Symbol> = markets.iter().map(|m| m.symbol.clone()).collect();
    let channels = create_channels(&symbols);
    let manager = build_data_manager(&registry, channels.market_data_tx.clone());

    let socket_path = monitor_socket_path(&settings);
    let current_state: Arc<std::sync::RwLock<Option<MonitorState>>> =
        Arc::new(std::sync::RwLock::new(None));
    let paused = Arc::new(AtomicBool::new(false));
    let emergency = Arc::new(AtomicBool::new(false));
    let pre_emergency_paused = Arc::new(AtomicBool::new(false));
    let command_center = Arc::new(Mutex::new(CommandCenter::default()));

    let socket_server = MonitorSocketServer::new_with_current_state(
        socket_path.clone(),
        Arc::clone(&current_state),
    );
    let monitor_client_count = socket_server.client_count_handle();
    let state_broadcaster = socket_server.state_broadcaster();
    let event_broadcaster = socket_server.event_broadcaster();
    let command_center_clone = Arc::clone(&command_center);
    tokio::spawn(async move {
        let handler = move |command_id: String, kind: CommandKind| {
            let now_ms = now_millis();
            let mut center = command_center_clone
                .lock()
                .map_err(|_| "命令中心锁异常".to_owned())?;
            let command = match kind {
                CommandKind::CancelCommand { command_id: target } => {
                    center.cancel(command_id, target, now_ms)
                }
                other => center.enqueue(command_id, other, now_ms),
            };
            Ok(CommandCenter::ack_message(&command))
        };
        if let Err(e) = socket_server.run(Box::new(handler)).await {
            tracing::error!("监控 Socket 服务异常: {}", e);
        }
    });

    let poly_source = PolymarketLiveDataSource::new(
        manager.clone(),
        settings.polymarket.clob_api_url.clone(),
        settings.strategy.settlement.clone(),
    );
    let mut cex_sources: HashMap<Exchange, CexLiveSource> = HashMap::new();
    for market in &markets {
        if !cex_sources.contains_key(&market.hedge_exchange) {
            cex_sources.insert(
                market.hedge_exchange,
                build_cex_source(manager.clone(), market, &settings),
            );
        }
    }

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let engine_config = build_paper_engine_config(&settings);
    let market_overrides = build_market_overrides(&settings, &markets);

    if !market_overrides.is_empty() {
        println!("=== 分币种参数覆盖（行情=WS）===");
        for market in &markets {
            let entry_z = market_overrides
                .get(&market.symbol)
                .and_then(|o| o.entry_z)
                .unwrap_or(engine_config.entry_z);
            println!(
                "  {} ({}) 入场Z = {}",
                market.symbol.0, market.cex_symbol, entry_z
            );
        }
        println!();
    }

    let mut engine = SimpleAlphaEngine::with_markets(engine_config.clone(), markets.clone());
    engine.set_market_overrides(market_overrides);
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let executor_start_ms = now_millis();
    let (orderbook_provider, executor, mut execution) = build_paper_execution_stack(
        &settings,
        &registry,
        executor_start_ms,
        execution_mode,
        None,
    )
    .await?;
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut observed: HashMap<Symbol, ObservedState> = markets
        .iter()
        .map(|m| (m.symbol.clone(), ObservedState::default()))
        .collect();
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::with_capacity(MAX_EVENT_LOGS);
    let mut last_event_seq_sent = 0_u64;
    let mut snapshots = Vec::new();
    let mut trade_book = TradeBook::default();
    let start_time_ms = executor_start_ms;
    let mut audit = if settings.audit.enabled {
        let audit = PaperAudit::start(&settings, env, &markets, executor_start_ms, trading_mode)?;
        println!(
            "审计会话已启动：{} -> {}",
            audit.session_id(),
            audit.session_dir().display()
        );
        Some(audit)
    } else {
        None
    };
    let (bootstrap_progress_tx, mut bootstrap_progress_rx) =
        mpsc::unbounded_channel::<MultiMarketBootstrapProgress>();
    let (warmup_tx, mut warmup_rx) = mpsc::channel::<MultiMarketWarmupEvent>(8);
    let funding_targets = collect_unique_funding_targets(&markets);
    let (funding_refresh_tx, mut funding_refresh_rx) =
        mpsc::unbounded_channel::<FundingRefreshResult>();
    let mut funding_refresh_task: Option<JoinHandle<()>> = None;
    let min_warmup_samples = settings.strategy.basis.min_warmup_samples.max(2);
    let mut warmup_status_by_symbol: HashMap<Symbol, MarketWarmupStatus> = HashMap::new();
    let mut last_strategy_readiness_event_ms: Option<u64> = None;

    push_event_simple(
        &mut recent_events,
        "market_data",
        format!(
            "{} 个市场已装载，实例先行启动，行情与快照将在后台逐步补齐",
            markets.len()
        ),
    );
    if include_funding {
        push_event_simple(
            &mut recent_events,
            "funding",
            format!(
                "资金费刷新已改为后台执行：{} 个市场按 {} 个交易所标的去重",
                markets.len(),
                funding_targets.len()
            ),
        );
    }
    let initial_monitor_events = build_monitor_events(&recent_events);
    let mut initial_monitor_state = build_multi_monitor_state(
        &settings,
        &markets,
        &engine,
        &observed,
        &stats,
        &risk,
        &execution,
        &mut trade_book,
        &command_center,
        paused.load(Ordering::SeqCst),
        emergency.load(Ordering::SeqCst),
        start_time_ms,
        now_millis(),
        &initial_monitor_events,
        trading_mode,
    );
    apply_strategy_health_to_monitor_state(
        &mut initial_monitor_state,
        &warmup_status_by_symbol,
        min_warmup_samples,
        0,
        0,
        0,
    );
    if let Ok(mut guard) = current_state.write() {
        *guard = Some(initial_monitor_state.clone());
    }
    let _ = state_broadcaster.send(initial_monitor_state);

    let monitor_market_cache: Arc<std::sync::RwLock<HashMap<Symbol, MonitorMarketSeed>>> =
        Arc::new(std::sync::RwLock::new(HashMap::new()));
    refresh_multi_monitor_cache(
        &monitor_market_cache,
        &symbols,
        &observed,
        &engine,
        &execution,
        &risk,
    );
    let (monitor_render_tx, mut monitor_render_rx) =
        mpsc::channel::<MultiMonitorRenderRequest>(1);
    let (monitor_render_result_tx, mut monitor_render_result_rx) =
        mpsc::channel::<MultiMonitorRenderResult>(1);
    let monitor_market_cache_clone = Arc::clone(&monitor_market_cache);
    let mut monitor_render_task = Some(tokio::spawn(async move {
        while let Some(request) = monitor_render_rx.recv().await {
            let result = {
                let cache = monitor_market_cache_clone
                    .read()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                build_multi_monitor_state_from_seed_cache(&request, &cache)
            };
            if monitor_render_result_tx.send(result).await.is_err() {
                break;
            }
        }
    }));
    let mut monitor_render_in_flight = false;

    let mut ws_tasks = spawn_ws_market_data_tasks(
        manager.clone(),
        &settings,
        &markets,
        depth,
        stats.polymarket_ws_diagnostics.clone(),
    );

    let mut warmup_task: Option<JoinHandle<()>> = None;
    let mut warmup_completed_markets = 0usize;
    let mut warmup_failed_markets = 0usize;
    let mut warmup_total_markets = 0usize;
    if warmup_klines > 0 {
        if markets.len() <= MULTI_MARKET_SYNC_WARMUP_THRESHOLD {
            warmup_total_markets = markets.len();
            for market in &markets {
                if let Some(source) = cex_sources.get(&market.hedge_exchange) {
                    match fetch_market_warmup_data(market, source, &poly_source, warmup_klines)
                        .await
                    {
                        Ok(data) => {
                            let status = market_warmup_status(&data);
                            if !status.poly_history_errors.is_empty() {
                                push_event_with_context(
                                    &mut recent_events,
                                    "error",
                                    format!(
                                        "{} 历史预热异常：Poly history 抓取失败",
                                        market.symbol.0
                                    ),
                                    Some(market.symbol.0.clone()),
                                    None,
                                    None,
                                    Some(market_warmup_event_details(&market.symbol.0, &status)),
                                );
                            }
                            warmup_status_by_symbol.insert(market.symbol.clone(), status);
                            apply_market_warmup_data(market, &mut engine, &data);
                            warmup_completed_markets += 1;
                        }
                        Err(err) => {
                            warmup_completed_markets += 1;
                            warmup_failed_markets += 1;
                            let status = MarketWarmupStatus {
                                cex_fetch_error: Some(err.to_string()),
                                ..MarketWarmupStatus::default()
                            };
                            warmup_status_by_symbol.insert(market.symbol.clone(), status.clone());
                            push_event_with_context(
                                &mut recent_events,
                                "error",
                                format!("{} 历史预热失败：CEX history 抓取失败", market.symbol.0),
                                Some(market.symbol.0.clone()),
                                None,
                                None,
                                Some(market_warmup_event_details(&market.symbol.0, &status)),
                            );
                        }
                    }
                }
            }
        } else {
            warmup_task = Some(spawn_multi_market_warmup_fetch(
                markets.clone(),
                poly_source.clone(),
                cex_sources.clone(),
                warmup_klines,
                warmup_tx,
            ));
            push_event_simple(
                &mut recent_events,
                "warmup",
                format!(
                    "{} 个市场改为后台历史预热，监控会先可用，策略随预热逐步就绪",
                    markets.len()
                ),
            );
        }
    }

    let mut bootstrap_in_flight = true;
    let mut bootstrap_task = Some(spawn_multi_market_snapshot_bootstrap(
        MultiMarketBootstrapPhase::Startup,
        markets.clone(),
        poly_source.clone(),
        cex_sources.clone(),
        depth,
        include_funding,
        bootstrap_progress_tx.clone(),
    ));

    let mut housekeeping = ws_housekeeping_interval(poll_interval_ms);
    housekeeping.tick().await;
    let mut snapshot_refresh = interval(Duration::from_secs(
        settings.strategy.market_data.snapshot_refresh_secs.max(1),
    ));
    snapshot_refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    snapshot_refresh.tick().await;
    let mut monitor_publish = interval(Duration::from_millis(MONITOR_PUBLISH_THROTTLE_MS));
    monitor_publish.set_missed_tick_behavior(MissedTickBehavior::Skip);
    monitor_publish.tick().await;
    let mut funding_poll = interval(Duration::from_secs(
        settings.strategy.market_data.funding_poll_secs.max(1),
    ));
    funding_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    funding_poll.tick().await;
    let mut state_dirty = true;
    let mut market_data_events_since_last_tick = 0_u64;

    let loop_result: Result<()> = async {
        loop {
            tokio::select! {
            Some(rendered) = monitor_render_result_rx.recv() => {
                monitor_render_in_flight = false;
                if let Some(alert) = rendered.readiness_alert.clone() {
                    push_event_with_context(
                        &mut recent_events,
                        alert.kind,
                        alert.summary,
                        None,
                        None,
                        None,
                        alert.details,
                    );
                    last_strategy_readiness_event_ms = Some(rendered.now_ms);
                    state_dirty = true;
                }
                if let Some(audit) = audit.as_mut() {
                    audit.maybe_record_runtime_state(
                        &rendered.stats,
                        &rendered.monitor_state,
                        rendered.tick_index,
                        rendered.now_ms,
                    )?;
                }
                if rendered.has_monitor_clients {
                    if let Ok(mut guard) = current_state.write() {
                        *guard = Some(rendered.monitor_state.clone());
                    }
                    let _ = state_broadcaster.send(rendered.monitor_state);
                } else if let Ok(mut guard) = current_state.write() {
                    *guard = Some(rendered.monitor_state);
                }
            }
            Some(result) = funding_refresh_rx.recv() => {
                funding_refresh_task = None;
                handle_funding_refresh_result(&mut stats, &mut recent_events, result);
                state_dirty = true;
            }
            Some(event) = warmup_rx.recv() => {
                match event {
                    MultiMarketWarmupEvent::Started {
                        total_markets,
                        concurrency,
                    } => {
                        warmup_total_markets = total_markets;
                        push_event_simple(
                            &mut recent_events,
                            "warmup",
                            format!(
                                "后台历史预热已启动：共 {} 个市场，并发 {}",
                                total_markets, concurrency
                            ),
                        );
                    }
                    MultiMarketWarmupEvent::MarketData { market, data } => {
                        let status = market_warmup_status(&data);
                        if !status.poly_history_errors.is_empty() {
                            push_event_with_context(
                                &mut recent_events,
                                "error",
                                format!("{} 历史预热异常：Poly history 抓取失败", market.symbol.0),
                                Some(market.symbol.0.clone()),
                                None,
                                None,
                                Some(market_warmup_event_details(&market.symbol.0, &status)),
                            );
                        }
                        warmup_status_by_symbol.insert(market.symbol.clone(), status);
                        apply_market_warmup_data(&market, &mut engine, &data);
                        refresh_multi_monitor_cache(
                            &monitor_market_cache,
                            std::slice::from_ref(&market.symbol),
                            &observed,
                            &engine,
                            &execution,
                            &risk,
                        );
                        if let Some(message) = market_warmup_diagnostic_message(&market, &data) {
                            println!("{message}");
                            push_event_simple(&mut recent_events, "warmup", message);
                        }
                        warmup_completed_markets += 1;
                        if warmup_completed_markets == warmup_total_markets
                            || warmup_completed_markets
                                % multi_market_warmup_progress_step(warmup_total_markets.max(1))
                                == 0
                        {
                            push_event_simple(
                                &mut recent_events,
                                "warmup",
                                format!(
                                    "后台历史预热进行中：{}/{} 个市场，失败 {}",
                                    warmup_completed_markets,
                                    warmup_total_markets,
                                    warmup_failed_markets,
                                ),
                            );
                        }
                    }
                    MultiMarketWarmupEvent::MarketFailed { symbol, error } => {
                        warmup_completed_markets += 1;
                        warmup_failed_markets += 1;
                        let status = MarketWarmupStatus {
                            cex_fetch_error: Some(error.clone()),
                            ..MarketWarmupStatus::default()
                        };
                        warmup_status_by_symbol.insert(Symbol::new(&symbol), status.clone());
                        push_event_with_context(
                            &mut recent_events,
                            "error",
                            format!("{symbol} 历史预热失败：CEX history 抓取失败"),
                            Some(symbol.clone()),
                            None,
                            None,
                            Some(market_warmup_event_details(&symbol, &status)),
                        );
                    }
                    MultiMarketWarmupEvent::Finished {
                        total_markets,
                        failed_markets,
                        duration_ms,
                    } => {
                        warmup_task = None;
                        push_event_simple(
                            &mut recent_events,
                            "warmup",
                            format!(
                                "后台历史预热完成：{}/{} 个市场，失败 {}，耗时 {:.1}s",
                                total_markets.saturating_sub(failed_markets),
                                total_markets,
                                failed_markets,
                                duration_ms as f64 / 1000.0
                            ),
                        );
                    }
                }
                state_dirty = true;
            }
            Some(progress) = bootstrap_progress_rx.recv() => {
                match progress.duration_ms {
                    Some(duration_ms) => {
                        bootstrap_in_flight = false;
                        bootstrap_task = None;
                        stats.poll_errors += progress.poll_errors;
                        push_event_simple(
                            &mut recent_events,
                            "market_data",
                            format!(
                                "{}完成：{}/{} 个市场，{} 个轮询错误，{} 个市场缺少 Polymarket token_id，耗时 {:.1}s",
                                progress.phase.label_zh(),
                                progress.completed_markets,
                                progress.total_markets,
                                progress.poll_errors,
                                progress.skipped_poly_markets,
                                duration_ms as f64 / 1000.0
                            ),
                        );
                    }
                    None if progress.completed_markets == 0 => {
                        push_event_simple(
                            &mut recent_events,
                            "market_data",
                            format!(
                                "{}已转入后台：共 {} 个市场，并发 {}",
                                progress.phase.label_zh(),
                                progress.total_markets,
                                multi_market_bootstrap_concurrency(progress.total_markets),
                            ),
                        );
                    }
                    None => {
                        push_event_simple(
                            &mut recent_events,
                            "market_data",
                            format!(
                                "{}进行中：{}/{} 个市场，累计 {} 个轮询错误",
                                progress.phase.label_zh(),
                                progress.completed_markets,
                                progress.total_markets,
                                progress.poll_errors,
                            ),
                        );
                    }
                }
                state_dirty = true;
            }
            event = market_data_rx.recv() => {
                match event {
                    Ok(event) => {
                        let refresh_symbols =
                            monitor_cache_symbols_for_market_event(&event, &registry, &symbols);
                        market_data_events_since_last_tick =
                            market_data_events_since_last_tick.saturating_add(1);
                        handle_multi_market_event(
                            event,
                            &settings,
                            &registry,
                            &orderbook_provider,
                            &mut observed,
                            &mut stats,
                            &mut recent_events,
                            &mut engine,
                            &mut execution,
                            &mut risk,
                            &mut trade_book,
                            &paused,
                            &emergency,
                            bootstrap_in_flight,
                            &mut audit,
                        ).await?;
                        if !refresh_symbols.is_empty() {
                            refresh_multi_monitor_cache(
                                &monitor_market_cache,
                                &refresh_symbols,
                                &observed,
                                &engine,
                                &execution,
                                &risk,
                            );
                        }
                        state_dirty = true;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged_messages)) => {
                        record_market_data_rx_lag(&mut stats, lagged_messages as u64);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = housekeeping.tick() => {
                record_market_data_tick_drain(
                    &mut stats,
                    &mut market_data_events_since_last_tick,
                );
                stats.ticks_processed += 1;
                let tick_index = stats.ticks_processed;
                let now_ms = now_millis();
                let now_secs = now_ms / 1000;

                drain_command_events(&command_center, &mut recent_events);
                let commands_processed = process_pending_command_multi(
                    &command_center,
                    &paused,
                    &emergency,
                    &pre_emergency_paused,
                    &mut settings,
                    &markets,
                    &mut engine,
                    &mut execution,
                    &mut risk,
                    &mut stats,
                    &mut recent_events,
                    &mut trade_book,
                    &mut audit,
                    now_ms,
                ).await?;
                drain_command_events(&command_center, &mut recent_events);
                if commands_processed {
                    refresh_multi_monitor_cache(
                        &monitor_market_cache,
                        &symbols,
                        &observed,
                        &engine,
                        &execution,
                        &risk,
                    );
                }

                for market in &markets {
                    if next_market_lifecycle_phase(
                        observed.get(&market.symbol),
                        market,
                        now_secs,
                        &settings.strategy.settlement,
                    )
                    .is_some()
                    {
                        let _ = manager.publish_market_lifecycle(
                            &market.symbol,
                            now_secs,
                            now_ms,
                            &settings.strategy.settlement,
                        );
                    } else if let Some(state) = observed.get_mut(&market.symbol) {
                        state.market_phase = Some(current_market_lifecycle_phase(
                            market,
                            now_secs,
                            &settings.strategy.settlement,
                        ));
                    }
                }

                let snapshot = build_multi_snapshot(
                    &markets,
                    tick_index,
                    &engine,
                    &observed,
                    &stats,
                    &risk,
                    &execution,
                    &executor,
                )?;
                if tick_index % print_every.max(1) == 0 {
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
                snapshots.push(snapshot.clone());
                state_dirty = true;

                if max_ticks > 0 && tick_index >= max_ticks {
                    break;
                }
            }
            _ = monitor_publish.tick(), if state_dirty => {
                let now_ms = now_millis();
                let tick_index = stats.ticks_processed;
                broadcast_monitor_events_since(
                    &recent_events,
                    &mut last_event_seq_sent,
                    &event_broadcaster,
                );

                let monitor_client_count = *monitor_client_count.read().await;
                let has_monitor_clients = monitor_client_count > 0;
                let should_render =
                    should_build_monitor_state(monitor_client_count, audit.as_ref(), now_ms);
                if !should_render {
                    state_dirty = false;
                } else if !monitor_render_in_flight {
                    let monitor_events = build_monitor_events(&recent_events);
                    let request = build_monitor_render_request(
                        &settings,
                        &stats,
                        &observed,
                        &engine,
                        &risk,
                        &mut trade_book,
                        &command_center,
                        paused.load(Ordering::SeqCst),
                        emergency.load(Ordering::SeqCst),
                        start_time_ms,
                        now_ms,
                        &monitor_events,
                        trading_mode,
                        warmup_status_by_symbol.clone(),
                        min_warmup_samples,
                        warmup_total_markets,
                        warmup_completed_markets,
                        warmup_failed_markets,
                        has_monitor_clients,
                        last_strategy_readiness_event_ms
                            .map(|last_seen_ms| {
                                now_ms.saturating_sub(last_seen_ms)
                                    >= STRATEGY_READINESS_ALERT_INTERVAL_MS
                            })
                            .unwrap_or(true),
                        tick_index,
                        true,
                    );
                    if monitor_render_tx.try_send(request).is_ok() {
                        monitor_render_in_flight = true;
                        state_dirty = false;
                    }
                }
            }
            _ = snapshot_refresh.tick() => {
                if bootstrap_in_flight {
                    push_event_simple(
                        &mut recent_events,
                        "market_data",
                        "后台快照任务仍在运行，本轮重同步已跳过".to_owned(),
                    );
                    state_dirty = true;
                    continue;
                }

                stats.snapshot_resync_count += 1;
                bootstrap_in_flight = true;
                bootstrap_task = Some(spawn_multi_market_snapshot_bootstrap(
                    MultiMarketBootstrapPhase::SnapshotRefresh,
                    markets.clone(),
                    poly_source.clone(),
                    cex_sources.clone(),
                    depth,
                    false,
                    bootstrap_progress_tx.clone(),
                ));
                state_dirty = true;
            }
            _ = funding_poll.tick(), if include_funding => {
                if funding_refresh_task.is_some() {
                    push_event_simple(
                        &mut recent_events,
                        "funding",
                        "上一轮后台资金费刷新仍在运行，本轮已跳过".to_owned(),
                    );
                    state_dirty = true;
                    continue;
                }

                stats.funding_refresh_count += 1;
                funding_refresh_task = Some(spawn_funding_refresh(
                    cex_sources.clone(),
                    funding_targets.clone(),
                    funding_refresh_tx.clone(),
                ));
                state_dirty = true;
            }
        }
        }
        Ok(())
    }
    .await;

    abort_join_handles(&mut ws_tasks).await;
    abort_optional_join_handle(&mut bootstrap_task).await;
    abort_optional_join_handle(&mut warmup_task).await;
    abort_optional_join_handle(&mut funding_refresh_task).await;
    abort_optional_join_handle(&mut monitor_render_task).await;

    println!(
        "多市场模拟盘已完成，共处理 {} 个轮次。",
        stats.ticks_processed
    );
    let final_monitor_state = current_state
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| {
            let monitor_events = build_monitor_events(&recent_events);
            build_multi_monitor_state(
                &settings,
                &markets,
                &engine,
                &observed,
                &stats,
                &risk,
                &execution,
                &mut trade_book,
                &command_center,
                paused.load(Ordering::SeqCst),
                emergency.load(Ordering::SeqCst),
                start_time_ms,
                now_millis(),
                &monitor_events,
                trading_mode,
            )
        });
    if let Ok(mut guard) = current_state.write() {
        *guard = Some(final_monitor_state.clone());
    }
    let runtime_result: Result<()> = async {
        loop_result?;
        let final_snapshot = snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("纸面盘会话未生成任何快照"))?;
        let open_orders = executor
            .open_order_snapshots()?
            .into_iter()
            .map(order_view_from_snapshot)
            .collect();
        let hedge_exchange = markets
            .first()
            .map(|market| format!("{:?}", market.hedge_exchange))
            .unwrap_or_else(|| "Unknown".to_owned());
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode,
            env: env.to_owned(),
            market_index: 0,
            market: format!("multi:{}", markets.len()),
            hedge_exchange,
            market_data_mode: Some("ws".to_owned()),
            poll_interval_ms,
            depth,
            include_funding,
            ticks_processed: stats.ticks_processed,
            final_snapshot,
            open_orders,
            recent_events: recent_events.into_iter().collect(),
            snapshots,
        };
        let artifact_path = artifact_path(&settings, env, trading_mode)?;
        persist_artifact(&artifact_path, &artifact)?;
        println!(
            "{}结果已写入：{}",
            artifact_runtime_label(trading_mode),
            artifact_path.display()
        );
        Ok(())
    }
    .await;
    finish_runtime_with_audit(
        runtime_result,
        &mut audit,
        &stats,
        &final_monitor_state,
        stats.ticks_processed,
        now_millis(),
    )?;
    if let Some(audit) = audit.as_ref() {
        println!("审计会话已完成：{}", audit.session_dir().display());
    }
    Ok(())
}

fn build_paper_engine_config(settings: &Settings) -> SimpleEngineConfig {
    let mut config = SimpleEngineConfig::default();
    let basis = global_basis_config(settings);
    let configured_entry = basis.entry_z_score_threshold.to_f64().unwrap_or(2.0);
    let configured_exit = basis.exit_z_score_threshold.to_f64().unwrap_or(0.5);

    config.min_signal_samples = basis.min_warmup_samples.max(2);
    config.rolling_window_minutes = ((basis.rolling_window_secs / 60) as usize).max(2);
    config.entry_z = configured_entry.abs().max(0.1);
    config.exit_z = configured_exit.abs().min(config.entry_z).max(0.05);
    config.position_notional_usd = basis.max_position_usd;
    config.cex_hedge_ratio = 1.0;
    config.dmm_quote_size = PolyShares::ZERO;
    config.enable_freshness_check = basis.enable_freshness_check;
    config.reject_on_disconnect = basis.reject_on_disconnect;
    let ws_poly_age_ms = settings
        .strategy
        .market_data
        .resolved_poly_open_max_quote_age_ms();
    let ws_cex_age_ms = settings
        .strategy
        .market_data
        .resolved_cex_open_max_quote_age_ms();
    config.max_poly_data_age_ms = match settings.strategy.market_data.mode {
        MarketDataMode::Ws => ws_poly_age_ms,
        MarketDataMode::Poll => basis.max_data_age_minutes.saturating_mul(60_000),
    };
    config.max_cex_data_age_ms = match settings.strategy.market_data.mode {
        MarketDataMode::Ws => ws_cex_age_ms,
        MarketDataMode::Poll => basis.max_data_age_minutes.saturating_mul(60_000),
    };
    config.max_time_diff_ms = basis.max_time_diff_minutes.saturating_mul(60_000);
    config
}

fn monitor_socket_path(settings: &Settings) -> String {
    settings.general.monitor_socket_path.clone()
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
    updated_at_ms: u64,
) {
    let name = format!("{exchange:?}");
    if status == ConnectionStatus::Connecting
        && observed
            .connections
            .get(&name)
            .is_some_and(|current| *current != ConnectionStatus::Connecting)
    {
        return;
    }
    record_connection_status(observed, name, status, updated_at_ms);
}

fn record_connection_status(
    observed: &mut ObservedState,
    name: String,
    status: ConnectionStatus,
    updated_at_ms: u64,
) {
    let previous = observed.connections.insert(name.clone(), status);
    observed
        .connection_updated_at_ms
        .insert(name.clone(), updated_at_ms);

    match (previous, status) {
        (Some(ConnectionStatus::Connected), ConnectionStatus::Disconnected)
        | (Some(ConnectionStatus::Connected), ConnectionStatus::Reconnecting) => {
            *observed
                .connection_disconnect_count
                .entry(name.clone())
                .or_insert(0) += 1;
            observed
                .connection_last_disconnect_at_ms
                .insert(name, updated_at_ms);
        }
        (Some(ConnectionStatus::Disconnected), ConnectionStatus::Connected)
        | (Some(ConnectionStatus::Reconnecting), ConnectionStatus::Connected) => {
            *observed.connection_reconnect_count.entry(name).or_insert(0) += 1;
        }
        _ => {}
    }
}

fn record_connection_activity(
    observed: &mut ObservedState,
    exchange: Exchange,
    updated_at_ms: u64,
) {
    observed
        .connection_updated_at_ms
        .insert(connection_name(exchange), updated_at_ms);
}

fn should_apply_observed_update(
    current: Option<ObservedDataVersion>,
    next: ObservedDataVersion,
) -> bool {
    current.is_none_or(|current| next >= current)
}

fn update_observed_leg(
    observed: &mut ObservedState,
    instrument: InstrumentKind,
    mid: Decimal,
    updated_at_ms: u64,
    version: ObservedDataVersion,
) {
    match instrument {
        InstrumentKind::PolyYes => {
            if should_apply_observed_update(observed.poly_yes_version, version) {
                observed.poly_yes_mid = Some(mid);
                observed.poly_yes_updated_at_ms = Some(updated_at_ms);
                observed.poly_yes_version = Some(version);
            }
        }
        InstrumentKind::PolyNo => {
            if should_apply_observed_update(observed.poly_no_version, version) {
                observed.poly_no_mid = Some(mid);
                observed.poly_no_updated_at_ms = Some(updated_at_ms);
                observed.poly_no_version = Some(version);
            }
        }
        InstrumentKind::CexPerp => {
            if should_apply_observed_update(observed.cex_version, version) {
                observed.cex_mid = Some(mid);
                observed.cex_updated_at_ms = Some(updated_at_ms);
                observed.cex_version = Some(version);
            }
        }
    }
}

fn observe_market_event(observed: &mut ObservedState, event: &MarketDataEvent) {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => {
            record_connection_activity(observed, snapshot.exchange, snapshot.received_at_ms);
            if let Some(mid) = mid_from_book(snapshot) {
                update_observed_leg(
                    observed,
                    snapshot.instrument,
                    mid,
                    snapshot.received_at_ms,
                    ObservedDataVersion {
                        exchange_timestamp_ms: snapshot.exchange_timestamp_ms,
                        sequence: snapshot.sequence,
                        received_at_ms: snapshot.received_at_ms,
                    },
                );
            }
        }
        MarketDataEvent::TradeUpdate {
            exchange,
            instrument,
            price,
            timestamp_ms,
            ..
        } => {
            record_connection_activity(observed, *exchange, *timestamp_ms);
            update_observed_leg(
                observed,
                *instrument,
                price.0,
                *timestamp_ms,
                ObservedDataVersion {
                    exchange_timestamp_ms: *timestamp_ms,
                    sequence: 0,
                    received_at_ms: *timestamp_ms,
                },
            );
        }
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange,
            bids,
            asks,
            exchange_timestamp_ms,
            received_at_ms,
            sequence,
            ..
        } => {
            record_connection_activity(observed, *exchange, *received_at_ms);
            if let Some(mid) = mid_from_price_levels(bids, asks) {
                update_observed_leg(
                    observed,
                    InstrumentKind::CexPerp,
                    mid,
                    *received_at_ms,
                    ObservedDataVersion {
                        exchange_timestamp_ms: *exchange_timestamp_ms,
                        sequence: *sequence,
                        received_at_ms: *received_at_ms,
                    },
                );
            }
        }
        MarketDataEvent::CexVenueTradeUpdate {
            exchange,
            price,
            timestamp_ms,
            ..
        } => {
            record_connection_activity(observed, *exchange, *timestamp_ms);
            update_observed_leg(
                observed,
                InstrumentKind::CexPerp,
                price.0,
                *timestamp_ms,
                ObservedDataVersion {
                    exchange_timestamp_ms: *timestamp_ms,
                    sequence: 0,
                    received_at_ms: *timestamp_ms,
                },
            );
        }
        MarketDataEvent::CexVenueFundingRate { exchange, .. } => {
            record_connection_activity(observed, *exchange, now_millis());
        }
        MarketDataEvent::FundingRate { exchange, .. } => {
            record_connection_activity(observed, *exchange, now_millis());
        }
        MarketDataEvent::MarketLifecycle { phase, .. } => {
            observed.market_phase = Some(phase.clone());
        }
        MarketDataEvent::ConnectionEvent { exchange, status } => {
            let name = format!("{exchange:?}");
            record_connection_status(observed, name, *status, now_millis());
        }
    }
}

fn mid_from_book(snapshot: &polyalpha_core::OrderBookSnapshot) -> Option<Decimal> {
    mid_from_price_levels(&snapshot.bids, &snapshot.asks)
}

fn mid_from_price_levels(
    bids: &[polyalpha_core::PriceLevel],
    asks: &[polyalpha_core::PriceLevel],
) -> Option<Decimal> {
    let best_bid = bids
        .iter()
        .max_by(|left, right| left.price.cmp(&right.price))
        .map(|level| level.price.0);
    let best_ask = asks
        .iter()
        .min_by(|left, right| left.price.cmp(&right.price))
        .map(|level| level.price.0);

    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::new(2, 0)),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
        (None, None) => None,
    }
}

fn build_snapshot(
    market: &MarketConfig,
    tick_index: usize,
    engine: &SimpleAlphaEngine,
    observed: &ObservedState,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    executor: &RuntimeExecutor,
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
    trade_book: &mut TradeBook,
    trigger: Option<&RuntimeTrigger>,
    events: Vec<ExecutionEvent>,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    for event in events {
        if let Some(audit) = audit.as_mut() {
            audit.record_execution(&event, trigger)?;
        }
        match &event {
            ExecutionEvent::TradePlanCreated { plan } => {
                trade_book.record_trade_plan(plan);
                push_event_with_context_and_payload(
                    recent_events,
                    "plan",
                    summarize_execution_event(&event),
                    Some(plan.symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    Some(plan.correlation_id.clone()),
                    build_execution_event_details(&event, trigger),
                    execution_event_monitor_payload(&event, trigger),
                );
            }
            ExecutionEvent::PlanSuperseded { symbol, .. } => {
                let (_, correlation_id) = audit_execution_identity(&event, trigger);
                push_event_with_context(
                    recent_events,
                    "plan",
                    summarize_execution_event(&event),
                    Some(symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    correlation_id,
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::RecoveryPlanCreated { plan } => {
                trade_book.record_trade_plan(plan);
                push_event_with_context_and_payload(
                    recent_events,
                    "recovery",
                    summarize_execution_event(&event),
                    Some(plan.symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    Some(plan.correlation_id.clone()),
                    build_execution_event_details(&event, trigger),
                    execution_event_monitor_payload(&event, trigger),
                );
            }
            ExecutionEvent::ExecutionResultRecorded { result } => {
                if !result.actual_funding_cost_usd.0.is_zero() {
                    let funding_adjustment =
                        UsdNotional(Decimal::ZERO - result.actual_funding_cost_usd.0);
                    risk.apply_realized_pnl_adjustment(UsdNotional(funding_adjustment.0))
                        .await?;
                    stats.total_pnl_usd += funding_adjustment.0.to_f64().unwrap_or_default();
                }
                push_event_with_context_and_payload(
                    recent_events,
                    "result",
                    summarize_execution_event(&event),
                    Some(result.symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    Some(result.correlation_id.clone()),
                    build_execution_event_details(&event, trigger),
                    execution_event_monitor_payload(&event, trigger),
                );
            }
            ExecutionEvent::OrderSubmitted {
                symbol,
                correlation_id,
                ..
            } => {
                stats.order_submitted += 1;
                let correlation_id = (!correlation_id.is_empty()).then(|| correlation_id.clone());
                push_event_with_context(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                    Some(symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    correlation_id,
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::OrderFilled(fill) => {
                stats.fills += 1;
                risk.on_fill(fill).await?;
                trade_book.record_fill(trigger, fill);
                push_event_with_context(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                    Some(fill.symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    Some(fill.correlation_id.clone()),
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::OrderCancelled { .. } => {
                stats.order_cancelled += 1;
                let (market, correlation_id) = audit_execution_identity(&event, trigger);
                push_event_with_context(
                    recent_events,
                    "execution",
                    summarize_execution_event(&event),
                    market,
                    trigger.map(|trigger| trigger.id().to_owned()),
                    correlation_id,
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::HedgeStateChanged { .. } => {
                stats.state_changes += 1;
                let (market, correlation_id) = audit_execution_identity(&event, trigger);
                push_event_with_context(
                    recent_events,
                    "state",
                    summarize_execution_event(&event),
                    market,
                    trigger.map(|trigger| trigger.id().to_owned()),
                    correlation_id,
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::ReconcileRequired { .. } => {
                let (market, correlation_id) = audit_execution_identity(&event, trigger);
                push_event_with_context(
                    recent_events,
                    "reconcile",
                    summarize_execution_event(&event),
                    market,
                    trigger.map(|trigger| trigger.id().to_owned()),
                    correlation_id,
                    build_execution_event_details(&event, trigger),
                );
            }
            ExecutionEvent::TradeClosed {
                symbol,
                correlation_id,
                realized_pnl,
                timestamp_ms,
            } => {
                stats.trades_closed += 1;
                trade_book.close_trade(
                    symbol,
                    *timestamp_ms,
                    realized_pnl.0.to_f64().unwrap_or(0.0),
                    Some(correlation_id.clone()),
                );
                push_event_with_context(
                    recent_events,
                    "trade_closed",
                    summarize_execution_event(&event),
                    Some(symbol.0.clone()),
                    trigger.map(|trigger| trigger.id().to_owned()),
                    Some(correlation_id.clone()),
                    build_execution_event_details(&event, trigger),
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
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange,
            venue_symbol,
            ..
        } => format!(
            "订单簿更新 {} {}（交易所聚合）",
            format_exchange_label(*exchange),
            venue_symbol
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
        MarketDataEvent::CexVenueTradeUpdate {
            exchange,
            venue_symbol,
            price,
            ..
        } => format!(
            "成交更新 {} {}（交易所聚合）@ {}",
            format_exchange_label(*exchange),
            venue_symbol,
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
        MarketDataEvent::CexVenueFundingRate {
            exchange,
            venue_symbol,
            rate,
            ..
        } => format!(
            "资金费更新 {} {} {}（交易所聚合）",
            format_exchange_label(*exchange),
            venue_symbol,
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
            "{} 本轮跳过：{}（Polymarket={}, 交易所={}）",
            symbol.0,
            EvaluationSkipReason::ConnectionLost.label_zh(),
            poly_connected,
            cex_connected
        ),
        EngineWarning::NoCexData { symbol } => {
            format!(
                "{} 本轮跳过：{}",
                symbol.0,
                EvaluationSkipReason::NoCexReference.label_zh()
            )
        }
        EngineWarning::NoPolyData { symbol } => {
            format!(
                "{} 本轮跳过：{}",
                symbol.0,
                EvaluationSkipReason::NoFreshPolySample.label_zh()
            )
        }
        EngineWarning::CexPriceStale {
            symbol,
            cex_age_ms,
            max_age_ms,
        } => format!(
            "{} 本轮跳过：{}（当前 {}ms，阈值 {}ms）",
            symbol.0,
            EvaluationSkipReason::CexPriceStale.label_zh(),
            cex_age_ms,
            max_age_ms
        ),
        EngineWarning::PolyPriceStale {
            symbol,
            poly_age_ms,
            max_age_ms,
        } => format!(
            "{} 本轮跳过：{}（当前 {}ms，阈值 {}ms）",
            symbol.0,
            EvaluationSkipReason::PolyPriceStale.label_zh(),
            poly_age_ms,
            max_age_ms
        ),
        EngineWarning::DataMisaligned {
            symbol, diff_ms, ..
        } => format!(
            "{} 本轮跳过：{}（差值 {}ms）",
            symbol.0,
            EvaluationSkipReason::DataMisaligned.label_zh(),
            diff_ms
        ),
    }
}

fn summarize_candidate(candidate: &OpenCandidate) -> String {
    format!(
        "{} {}",
        candidate_kind_label_zh(candidate.direction.as_str()),
        candidate.candidate_id
    )
}

fn trim_float_string(mut value: String) -> String {
    if value.contains('.') {
        while value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
    }
    value
}

fn format_detail_f64(value: f64) -> String {
    trim_float_string(format!("{value:.6}"))
}

fn format_detail_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn format_detail_ms(value: u64) -> String {
    format!("{value}ms")
}

fn format_detail_timestamp(timestamp_ms: u64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms as i64)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn bool_zh(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

fn execution_pipeline_payload(
    candidate: Option<OpenCandidate>,
    intent: Option<PlanningIntent>,
    plan: Option<TradePlan>,
    result: Option<ExecutionResult>,
    plan_rejection_reason: Option<String>,
    revalidation_failure_reason: Option<String>,
    recovery_decision_reason: Option<String>,
) -> MonitorEventPayload {
    MonitorEventPayload::ExecutionPipeline(ExecutionPipelineView {
        schema_version: PLANNING_SCHEMA_VERSION,
        candidate,
        intent,
        plan,
        result,
        plan_rejection_reason,
        revalidation_failure_reason,
        recovery_decision_reason,
    })
}

fn candidate_monitor_payload(candidate: &OpenCandidate) -> MonitorEventPayload {
    execution_pipeline_payload(Some(candidate.clone()), None, None, None, None, None, None)
}

fn execution_event_monitor_payload(
    event: &ExecutionEvent,
    trigger: Option<&RuntimeTrigger>,
) -> Option<MonitorEventPayload> {
    let (candidate, intent) = match trigger {
        Some(RuntimeTrigger::Candidate(candidate)) => (Some(candidate.clone()), None),
        Some(RuntimeTrigger::Intent { intent, .. }) => (None, Some(intent.clone())),
        None => (None, None),
    };

    match event {
        ExecutionEvent::TradePlanCreated { plan } => Some(execution_pipeline_payload(
            candidate,
            intent,
            Some(plan.clone()),
            None,
            None,
            None,
            None,
        )),
        ExecutionEvent::RecoveryPlanCreated { plan } => Some(execution_pipeline_payload(
            candidate,
            intent,
            Some(plan.clone()),
            None,
            None,
            None,
            plan.recovery_decision_reason
                .clone()
                .map(|reason| reason.code().to_owned()),
        )),
        ExecutionEvent::ExecutionResultRecorded { result } => Some(execution_pipeline_payload(
            candidate,
            intent,
            None,
            Some(result.clone()),
            None,
            None,
            None,
        )),
        _ => None,
    }
}

fn extract_reason_code(message: &str, prefix: &str) -> Option<String> {
    let rest = message.strip_prefix(prefix)?;
    let (code, _) = rest.split_once(']')?;
    (!code.is_empty()).then(|| code.to_owned())
}

fn extract_direct_plan_rejection_reason(message: &str) -> Option<String> {
    extract_reason_code(message, "plan rejected [")
}

fn extract_revalidation_failure_reason(message: &str) -> Option<String> {
    extract_reason_code(message, "plan revalidation failed [")
}

fn extract_plan_rejection_reason(message: &str) -> Option<String> {
    extract_direct_plan_rejection_reason(message)
        .or_else(|| extract_revalidation_failure_reason(message))
}

fn post_submit_rejection_payload(
    candidate: &OpenCandidate,
    rejection_reasons: &[String],
) -> MonitorEventPayload {
    let plan_rejection_reason = rejection_reasons
        .iter()
        .find_map(|reason| extract_direct_plan_rejection_reason(reason));
    let revalidation_failure_reason = rejection_reasons
        .iter()
        .find_map(|reason| extract_revalidation_failure_reason(reason));
    execution_pipeline_payload(
        Some(candidate.clone()),
        None,
        None,
        None,
        plan_rejection_reason,
        revalidation_failure_reason,
        None,
    )
}

fn intent_rejection_payload(trigger: &RuntimeTrigger, message: &str) -> MonitorEventPayload {
    execution_pipeline_payload(
        None,
        trigger.intent().cloned(),
        None,
        None,
        extract_direct_plan_rejection_reason(message),
        extract_revalidation_failure_reason(message),
        None,
    )
}

fn maybe_details(details: EventDetails) -> Option<EventDetails> {
    (!details.is_empty()).then_some(details)
}

fn insert_detail(details: &mut EventDetails, key: &str, value: impl Into<String>) {
    let value = value.into();
    if !value.is_empty() {
        details.insert(key.to_owned(), value);
    }
}

fn insert_detail_opt(details: &mut EventDetails, key: &str, value: Option<String>) {
    if let Some(value) = value {
        insert_detail(details, key, value);
    }
}

fn candidate_kind_label_zh(direction: &str) -> &'static str {
    match direction {
        "long" => "开仓机会",
        "short" => "反向机会",
        _ => "候选机会",
    }
}

fn candidate_direction_zh(direction: &str) -> &'static str {
    match direction {
        "long" => "多",
        "short" => "空",
        _ => "未知",
    }
}

fn trigger_action_label_zh(trigger: &RuntimeTrigger) -> String {
    match trigger {
        RuntimeTrigger::Candidate(candidate) => {
            candidate_kind_label_zh(candidate.direction.as_str()).to_owned()
        }
        RuntimeTrigger::Intent { intent, .. } => match intent {
            PlanningIntent::OpenPosition { .. } => "准备开仓".to_owned(),
            PlanningIntent::ClosePosition { .. } => "准备平仓".to_owned(),
            PlanningIntent::DeltaRebalance { .. } => "补 CEX 对冲".to_owned(),
            PlanningIntent::ResidualRecovery { .. } => "处理剩余敞口".to_owned(),
            PlanningIntent::ForceExit { .. } => "强制退出".to_owned(),
        },
    }
}

fn intent_stage_label_zh(intent: &PlanningIntent) -> &'static str {
    match intent {
        PlanningIntent::OpenPosition { .. } => "准备开仓",
        PlanningIntent::ClosePosition { .. } => "准备平仓",
        PlanningIntent::DeltaRebalance { .. } => "补 CEX 对冲",
        PlanningIntent::ResidualRecovery { .. } => "处理剩余敞口",
        PlanningIntent::ForceExit { .. } => "强制退出",
    }
}

fn signal_strength_label_zh(strength: SignalStrength) -> &'static str {
    match strength {
        SignalStrength::Strong => "强",
        SignalStrength::Normal => "中",
        SignalStrength::Weak => "弱",
    }
}

fn gate_label_zh(gate: &str) -> &str {
    match gate {
        "ws_freshness" => "WS 行情新鲜度",
        "market_pool" => "市场池准入",
        "trade_plan" => "交易规划",
        "risk_guard" => "风控",
        "hedge_qty_guard" => "CEX 对冲量",
        "price_filter" => "价格过滤",
        "trading_pause" => "暂停保护",
        "emergency_stop" => "紧急停止",
        _ => gate,
    }
}

fn gate_reason_label_zh(gate: &str, reason: &str) -> String {
    match gate {
        "ws_freshness" => format_ws_freshness_reason(reason).to_owned(),
        "market_pool" => {
            if let Some(reason) = crate::market_pool::cooldown_reason_from_gate(reason) {
                return format!(
                    "市场处于冷却期（{}）",
                    translate_trade_plan_rejection(reason)
                );
            }
            if let Some(reason) = reason.strip_prefix("retained_") {
                return match reason {
                    "force_exit" => "市场当前处于强退流程".to_owned(),
                    "residual_recovery" => "市场当前处于剩余敞口处理流程".to_owned(),
                    "close_in_progress" => "市场当前处于平仓流程".to_owned(),
                    "delta_rebalance" => "市场当前处于补对冲流程".to_owned(),
                    "has_position" => "市场已有持仓，禁止重复开仓".to_owned(),
                    _ => "市场当前不允许新开仓".to_owned(),
                };
            }
            match reason {
                "not_tradeable_pool" => "市场当前不在交易池".to_owned(),
                _ => "市场当前不允许新开仓".to_owned(),
            }
        }
        "price_filter" => match reason {
            "ok" => "价格在允许区间".to_owned(),
            "close_position" => "平仓信号绕过价格过滤".to_owned(),
            "price_filter" => "价格不在允许区间".to_owned(),
            "below_min_poly_price" => "低于最小 Poly 价格带".to_owned(),
            "above_or_equal_max_poly_price" => "高于或等于最大 Poly 价格带".to_owned(),
            "missing_poly_quote" => "缺少 Poly 报价".to_owned(),
            "path_dependent_market_not_supported" => {
                "触达类题型暂未纳入主策略，禁止新开仓".to_owned()
            }
            "all_time_high_market_not_supported" => {
                "历史新高类题型暂未纳入主策略，禁止新开仓".to_owned()
            }
            "non_price_market_not_supported" => "非价格事件题暂未纳入主策略，禁止新开仓".to_owned(),
            "terminal_price_market_not_supported" => {
                "该价格题型未纳入主策略，禁止新开仓".to_owned()
            }
            _ => "价格过滤未通过".to_owned(),
        },
        "risk_guard" => {
            if reason == "ok" {
                "通过风控检查".to_owned()
            } else {
                translate_risk_rejection(reason)
            }
        }
        "trade_plan" => {
            if reason == "ok" {
                "通过交易规划".to_owned()
            } else {
                translate_trade_plan_rejection(reason)
            }
        }
        "hedge_qty_guard" => match reason {
            "ok" => "CEX 对冲量有效".to_owned(),
            "zero_cex_hedge_qty" => "CEX 对冲量为 0，禁止开仓".to_owned(),
            _ => "CEX 对冲量无效".to_owned(),
        },
        "trading_pause" => "交易已暂停".to_owned(),
        "emergency_stop" => "系统处于紧急停止".to_owned(),
        _ => reason.to_owned(),
    }
}

fn translate_risk_rejection(reason: &str) -> String {
    if reason == "circuit breaker is open" {
        return "熔断器已打开".to_owned();
    }
    if reason == "market is not tradable in the current phase" {
        return "当前市场阶段不允许新开仓".to_owned();
    }
    if reason == "persistence lag exceeded threshold" {
        return "持久化延迟超阈值".to_owned();
    }
    if reason == "quantity became zero after precision normalization" {
        return "下单数量在精度归一后变为 0".to_owned();
    }
    if reason == "zero_cex_hedge_qty" {
        return "CEX 对冲量为 0，禁止开仓".to_owned();
    }
    if let Some(limit_reason) = reason.strip_prefix("risk limit breached: ") {
        if limit_reason == "daily realized pnl is below max loss" {
            return "当日已实现盈亏触发止损线".to_owned();
        }
        if let Some(value) = limit_reason.strip_prefix("single-position exposure ") {
            if let Some((left, right)) = value.split_once(" > ") {
                return format!("单市场敞口 {left} 超过上限 {right}");
            }
        }
        if let Some(value) = limit_reason.strip_prefix("total exposure ") {
            if let Some((left, right)) = value.split_once(" > ") {
                return format!("总敞口 {left} 超过上限 {right}");
            }
        }
        return format!("触发风险限额：{limit_reason}");
    }
    reason.to_owned()
}

fn translate_trade_plan_rejection(reason: &str) -> String {
    match reason {
        "missing_poly_book" => "缺少 Poly 订单簿".to_owned(),
        "missing_cex_book" => "缺少 CEX 订单簿".to_owned(),
        "one_sided_poly_book" => "Poly 订单簿单边".to_owned(),
        "one_sided_cex_book" => "CEX 订单簿单边".to_owned(),
        "stale_book_sequence" => "订单簿序列回退".to_owned(),
        "book_conflict_same_sequence" => "同序列订单簿内容冲突".to_owned(),
        "non_positive_planned_edge" => "扣除成本后预期收益不足".to_owned(),
        "insufficient_poly_depth" => "Poly 深度不足".to_owned(),
        "insufficient_cex_depth" => "CEX 深度不足".to_owned(),
        "open_instant_loss_too_large" => "开仓瞬时亏损过大".to_owned(),
        "poly_max_price_exceeded" => "Poly 成交均价超出开仓上限".to_owned(),
        "poly_price_impact_too_large" => "Poly 吃单过深，价格冲击过大".to_owned(),
        "poly_min_proceeds_not_met" => "Poly 平仓回款低于下限".to_owned(),
        "zero_cex_hedge_qty" => "CEX 对冲量为 0，禁止开仓".to_owned(),
        "residual_delta_too_large" => "剩余敞口超过上限".to_owned(),
        "shock_loss_too_large" => "价格冲击风险超过上限".to_owned(),
        "market_phase_disallows_intent" => "当前市场阶段不允许该计划".to_owned(),
        "higher_priority_plan_active" => "已有更高优先级计划在执行".to_owned(),
        "adapter_cannot_preserve_constraints" => "交易适配层无法保留计划约束".to_owned(),
        "plan_ttl_expired" => "计划已过期".to_owned(),
        "poly_sequence_drift_exceeded" => "Poly 行情序列漂移超限".to_owned(),
        "cex_sequence_drift_exceeded" => "CEX 行情序列漂移超限".to_owned(),
        "poly_price_move_exceeded" => "Poly 行情变动超出容忍范围".to_owned(),
        "cex_price_move_exceeded" => "CEX 行情变动超出容忍范围".to_owned(),
        "planned_edge_deteriorated" => "计划边际恶化到阈值以下".to_owned(),
        "residual_risk_deteriorated" => "残余风险恶化到阈值以上".to_owned(),
        "plan_superseded" => "计划已被更高优先级计划覆盖".to_owned(),
        "book_missing_on_revalidate" => "重验时缺少订单簿".to_owned(),
        "book_conflict_on_revalidate" => "重验时订单簿冲突".to_owned(),
        _ => reason.to_owned(),
    }
}

fn candidate_price_for_audit_observed(
    candidate: &OpenCandidate,
    observed: &AuditObservedMarket,
) -> Option<f64> {
    match candidate.token_side {
        TokenSide::Yes => observed.poly_yes_mid,
        TokenSide::No => observed.poly_no_mid,
    }
}

fn price_filter_window_text_for_basis(basis: &EffectiveBasisConfig) -> String {
    if !basis.band_policy.uses_configured_band() {
        return "已禁用".to_owned();
    }
    let min_price = basis.min_poly_price.and_then(|value| value.to_f64());
    let max_price = basis.max_poly_price.and_then(|value| value.to_f64());
    match (min_price, max_price) {
        (Some(min), Some(max)) => {
            format!("{} - {}", format_detail_f64(min), format_detail_f64(max))
        }
        (Some(min), None) => format!(">= {}", format_detail_f64(min)),
        (None, Some(max)) => format!("<= {}", format_detail_f64(max)),
        (None, None) => "未配置".to_owned(),
    }
}

fn price_filter_window_text_for_symbol(settings: &Settings, symbol: &Symbol) -> String {
    price_filter_window_text_for_basis(&effective_basis_config_for_symbol(settings, symbol))
}

fn connection_summary_zh(connections: &HashMap<String, ConnectionStatus>) -> Option<String> {
    if connections.is_empty() {
        return None;
    }
    let mut items = connections
        .iter()
        .map(|(name, status)| {
            format!(
                "{}={}",
                display_exchange_text(name),
                format_connection_status_label(*status)
            )
        })
        .collect::<Vec<_>>();
    items.sort();
    Some(items.join("，"))
}

fn connection_idle_summary_zh(idles: &HashMap<String, u64>) -> Option<String> {
    if idles.is_empty() {
        return None;
    }
    let mut items = idles
        .iter()
        .map(|(name, idle_ms)| {
            format!(
                "{}={}",
                display_exchange_text(name),
                format_detail_ms(*idle_ms)
            )
        })
        .collect::<Vec<_>>();
    items.sort();
    Some(items.join("，"))
}

fn add_trigger_details(details: &mut EventDetails, trigger: &RuntimeTrigger) {
    insert_detail(details, "市场", trigger.symbol().0.clone());
    insert_detail(details, "触发ID", trigger.id().to_owned());
    insert_detail(details, "关联ID", trigger.correlation_id().to_owned());
    insert_detail(details, "动作", trigger_action_label_zh(trigger));
    insert_detail(
        details,
        "时间",
        format_detail_timestamp(trigger.timestamp_ms()),
    );

    if let Some(candidate) = trigger.candidate() {
        insert_detail(
            details,
            "强度",
            signal_strength_label_zh(candidate.strength),
        );
        insert_detail(
            details,
            "方向",
            candidate_direction_zh(candidate.direction.as_str()),
        );
        insert_detail(
            details,
            "买入方向",
            format_token_side_label(candidate.token_side),
        );
        insert_detail(
            details,
            "模型参考价",
            format_detail_f64(candidate.fair_value),
        );
        insert_detail(
            details,
            "模型价差",
            format_detail_f64(candidate.raw_mispricing),
        );
        insert_detail(
            details,
            "对冲系数",
            format_detail_f64(candidate.delta_estimate),
        );
        insert_detail(
            details,
            "预算上限USD",
            format_detail_f64(candidate.risk_budget_usd),
        );
        insert_detail_opt(details, "Z值", candidate.z_score.map(format_detail_f64));
        insert_detail(
            details,
            "参考价状态",
            candidate_reference_price_state_label(candidate),
        );
        insert_detail_opt(
            details,
            "当前参考波动",
            candidate.effective_sigma.map(format_detail_f64),
        );
        return;
    }

    if let Some(intent) = trigger.intent() {
        insert_detail(details, "当前流程", intent_stage_label_zh(intent));
        match intent {
            PlanningIntent::OpenPosition {
                max_budget_usd,
                max_residual_delta,
                max_shock_loss_usd,
                ..
            } => {
                insert_detail(details, "预算上限USD", format_detail_f64(*max_budget_usd));
                insert_detail(
                    details,
                    "剩余敞口上限",
                    format_detail_f64(*max_residual_delta),
                );
                insert_detail(
                    details,
                    "冲击亏损上限USD",
                    format_detail_f64(*max_shock_loss_usd),
                );
            }
            PlanningIntent::ClosePosition {
                close_reason,
                target_close_ratio,
                ..
            } => {
                insert_detail(details, "平仓原因", close_reason.clone());
                insert_detail(details, "平仓比例", format_detail_f64(*target_close_ratio));
            }
            PlanningIntent::DeltaRebalance {
                residual_snapshot,
                target_residual_delta_max,
                target_shock_loss_max,
                ..
            } => {
                insert_detail(
                    details,
                    "剩余敞口概况",
                    format_residual_snapshot_detail(residual_snapshot),
                );
                insert_detail(
                    details,
                    "剩余敞口上限",
                    format_detail_f64(*target_residual_delta_max),
                );
                insert_detail(
                    details,
                    "冲击亏损上限",
                    format_detail_f64(*target_shock_loss_max),
                );
            }
            PlanningIntent::ResidualRecovery {
                residual_snapshot,
                recovery_reason,
                ..
            } => {
                insert_detail(
                    details,
                    "剩余敞口概况",
                    format_residual_snapshot_detail(residual_snapshot),
                );
                insert_detail(
                    details,
                    "处理原因",
                    recovery_reason_label_zh(*recovery_reason),
                );
            }
            PlanningIntent::ForceExit {
                force_reason,
                allow_negative_edge,
                ..
            } => {
                insert_detail(details, "强退原因", force_reason.clone());
                insert_detail(details, "允许负边际", bool_zh(*allow_negative_edge));
            }
        }
    }
}

fn recovery_reason_label_zh(reason: RecoveryDecisionReason) -> &'static str {
    match reason {
        RecoveryDecisionReason::CexTopUpCheaper => "补 CEX 更便宜",
        RecoveryDecisionReason::PolyFlattenCheaper => "平 Poly 更便宜",
        RecoveryDecisionReason::CexTopUpFaster => "补 CEX 更快",
        RecoveryDecisionReason::PolyFlattenOnlySafeRoute => "只剩平 Poly 更安全",
        RecoveryDecisionReason::GhostOrderGuardBlocksFlatten => "挂单保护阻止平 Poly",
        RecoveryDecisionReason::MarketPhaseBlocksPoly => "当前阶段不允许平 Poly",
        RecoveryDecisionReason::CexDepthInsufficient => "CEX 深度不足",
        RecoveryDecisionReason::PolyDepthInsufficient => "Poly 深度不足",
        RecoveryDecisionReason::ForceExitRequired => "必须强制退出",
    }
}

fn candidate_reference_price_state_label(candidate: &OpenCandidate) -> String {
    match candidate.sigma_source.as_deref() {
        Some("realized") => {
            format!("已切换实时波动（样本 {} 笔）", candidate.returns_window_len)
        }
        _ if candidate.returns_window_len > 0 => format!(
            "参考价预热中（已积累 {} 笔，暂用默认波动）",
            candidate.returns_window_len
        ),
        _ => "参考价预热中（暂用默认波动）".to_owned(),
    }
}

fn format_residual_snapshot_detail(snapshot: &polyalpha_core::ResidualSnapshot) -> String {
    let preferred_side = match snapshot.preferred_cex_side {
        OrderSide::Buy => "买入",
        OrderSide::Sell => "卖出",
    };
    format!(
        "剩余敞口={}，当前CEX对冲={}，YES持仓={}，NO持仓={}，优先补对冲方向={}",
        format_detail_f64(snapshot.residual_delta),
        snapshot.planned_cex_qty.0.normalize(),
        snapshot.current_poly_yes_shares.0.normalize(),
        snapshot.current_poly_no_shares.0.normalize(),
        preferred_side
    )
}

fn add_observed_details(details: &mut EventDetails, observed: &AuditObservedMarket) {
    insert_detail_opt(
        details,
        "Polymarket YES",
        observed.poly_yes_mid.map(format_detail_f64),
    );
    insert_detail_opt(
        details,
        "Polymarket NO",
        observed.poly_no_mid.map(format_detail_f64),
    );
    insert_detail_opt(details, "CEX中价", observed.cex_mid.map(format_detail_f64));
    insert_detail_opt(
        details,
        "Polymarket更新时间",
        observed.poly_updated_at_ms.map(format_detail_timestamp),
    );
    insert_detail_opt(
        details,
        "CEX更新时间",
        observed.cex_updated_at_ms.map(format_detail_timestamp),
    );
    insert_detail_opt(
        details,
        "Polymarket报价龄",
        observed.poly_quote_age_ms.map(format_detail_ms),
    );
    insert_detail_opt(
        details,
        "CEX报价龄",
        observed.cex_quote_age_ms.map(format_detail_ms),
    );
    insert_detail_opt(
        details,
        "双腿时差",
        observed.cross_leg_skew_ms.map(format_detail_ms),
    );
    insert_detail(details, "评估状态", observed.evaluable_status.label_zh());
    insert_detail(
        details,
        "异步分类",
        observed.async_classification.label_zh(),
    );
    insert_detail_opt(details, "市场阶段", observed.market_phase.clone());
    insert_detail_opt(
        details,
        "连接状态",
        connection_summary_zh(&observed.connections),
    );
    insert_detail_opt(
        details,
        "连接空闲",
        connection_idle_summary_zh(&observed.transport_idle_ms_by_connection),
    );
}

fn add_risk_details(
    details: &mut EventDetails,
    risk: &InMemoryRiskManager,
    candidate: &OpenCandidate,
    rejection_reason: Option<&str>,
) {
    let snapshot = risk.build_snapshot(now_millis());
    insert_detail(
        details,
        "当前总敞口",
        format_detail_decimal(snapshot.total_exposure_usd.0),
    );
    insert_detail(
        details,
        "当前单市场敞口",
        format_detail_decimal(
            risk.position_tracker()
                .symbol_exposure_usd(&candidate.symbol)
                .0,
        ),
    );
    insert_detail(
        details,
        "当日已实现盈亏",
        format_detail_decimal(snapshot.daily_pnl.0),
    );
    insert_detail(
        details,
        "熔断器",
        display_circuit_breaker_text(&format!("{:?}", snapshot.circuit_breaker)),
    );
    insert_detail_opt(
        details,
        "熔断原因",
        risk.breaker_reason().map(|reason| reason.to_owned()),
    );
    insert_detail(
        details,
        "总敞口上限",
        format_detail_decimal(risk.limits().max_total_exposure_usd.0),
    );
    insert_detail(
        details,
        "单市场上限",
        format_detail_decimal(risk.limits().max_single_position_usd.0),
    );
    insert_detail(
        details,
        "日亏损上限",
        format_detail_decimal(risk.limits().max_daily_loss_usd.0),
    );
    insert_detail_opt(
        details,
        "市场阶段",
        risk.market_phase(&candidate.symbol)
            .map(|phase| format_market_phase_label(phase).to_owned()),
    );
    if let Some(reason) = rejection_reason {
        insert_detail(details, "风控拒绝", translate_risk_rejection(reason));
        insert_detail(details, "风控原文", reason.to_owned());
    }
}

fn add_trade_plan_details(details: &mut EventDetails, plan: &TradePlan) {
    insert_detail(details, "Schema版本", plan.schema_version.to_string());
    insert_detail(details, "计划哈希", plan.plan_hash.clone());
    insert_detail_opt(details, "父计划ID", plan.parent_plan_id.clone());
    insert_detail_opt(details, "替换前计划ID", plan.supersedes_plan_id.clone());
    insert_detail(details, "幂等键", plan.idempotency_key.clone());
    insert_detail(details, "Poly定价模式", plan.poly_sizing_mode.clone());
    insert_detail(
        details,
        "Poly请求股数",
        format_detail_decimal(plan.poly_requested_shares.0),
    );
    insert_detail(
        details,
        "Poly计划股数",
        format_detail_decimal(plan.poly_planned_shares.0),
    );
    insert_detail(
        details,
        "Poly预算上限USD",
        format_detail_decimal(plan.poly_max_cost_usd.0),
    );
    insert_detail(
        details,
        "Poly最大均价",
        format_detail_decimal(plan.poly_max_avg_price.0),
    );
    insert_detail(
        details,
        "Poly最小均价",
        format_detail_decimal(plan.poly_min_avg_price.0),
    );
    insert_detail(
        details,
        "Poly最小收入USD",
        format_detail_decimal(plan.poly_min_proceeds_usd.0),
    );
    insert_detail(
        details,
        "Poly簿面均价",
        format_detail_decimal(plan.poly_book_avg_price.0),
    );
    insert_detail(
        details,
        "Poly可成交价",
        format_detail_decimal(plan.poly_executable_price.0),
    );
    insert_detail(
        details,
        "Poly吃簿成本USD",
        format_detail_decimal(plan.poly_friction_cost_usd.0),
    );
    insert_detail(
        details,
        "Poly手续费USD",
        format_detail_decimal(plan.poly_fee_usd.0),
    );
    insert_detail(
        details,
        "CEX计划数量",
        format_detail_decimal(plan.cex_planned_qty.0),
    );
    insert_detail(
        details,
        "CEX簿面均价",
        format_detail_decimal(plan.cex_book_avg_price.0),
    );
    insert_detail(
        details,
        "CEX可成交价",
        format_detail_decimal(plan.cex_executable_price.0),
    );
    insert_detail(
        details,
        "CEX摩擦成本USD",
        format_detail_decimal(plan.cex_friction_cost_usd.0),
    );
    insert_detail(
        details,
        "CEX手续费USD",
        format_detail_decimal(plan.cex_fee_usd.0),
    );
    insert_detail(
        details,
        "理论边际USD",
        format_detail_decimal(plan.raw_edge_usd.0),
    );
    insert_detail(
        details,
        "计划边际USD",
        format_detail_decimal(plan.planned_edge_usd.0),
    );
    insert_detail(
        details,
        "Funding成本USD",
        format_detail_decimal(plan.expected_funding_cost_usd.0),
    );
    insert_detail(
        details,
        "残余风险惩罚USD",
        format_detail_decimal(plan.residual_risk_penalty_usd.0),
    );
    insert_detail(
        details,
        "计划残余Delta",
        format_detail_f64(plan.post_rounding_residual_delta),
    );
    insert_detail(
        details,
        "计划冲击亏损+1%USD",
        format_detail_decimal(plan.shock_loss_up_1pct.0),
    );
    insert_detail(
        details,
        "计划冲击亏损-1%USD",
        format_detail_decimal(plan.shock_loss_down_1pct.0),
    );
    insert_detail(
        details,
        "计划冲击亏损+2%USD",
        format_detail_decimal(plan.shock_loss_up_2pct.0),
    );
    insert_detail(
        details,
        "计划冲击亏损-2%USD",
        format_detail_decimal(plan.shock_loss_down_2pct.0),
    );
    insert_detail(details, "计划TTL", format_detail_ms(plan.plan_ttl_ms));
    insert_detail(details, "Poly序列", plan.poly_sequence.to_string());
    insert_detail(
        details,
        "Poly交易所时间",
        format_detail_timestamp(plan.poly_exchange_timestamp_ms),
    );
    insert_detail(
        details,
        "Poly接收时间",
        format_detail_timestamp(plan.poly_received_at_ms),
    );
    insert_detail(details, "CEX序列", plan.cex_sequence.to_string());
    insert_detail(
        details,
        "CEX交易所时间",
        format_detail_timestamp(plan.cex_exchange_timestamp_ms),
    );
    insert_detail(
        details,
        "CEX接收时间",
        format_detail_timestamp(plan.cex_received_at_ms),
    );
}

fn add_execution_result_details(details: &mut EventDetails, result: &ExecutionResult) {
    insert_detail(details, "Schema版本", result.schema_version.to_string());
    insert_detail(details, "结果状态", result.status.clone());
    insert_detail(
        details,
        "实际Poly成本USD",
        format_detail_decimal(result.actual_poly_cost_usd.0),
    );
    insert_detail(
        details,
        "实际CEX成本USD",
        format_detail_decimal(result.actual_cex_cost_usd.0),
    );
    insert_detail(
        details,
        "实际Poly手续费USD",
        format_detail_decimal(result.actual_poly_fee_usd.0),
    );
    insert_detail(
        details,
        "实际CEX手续费USD",
        format_detail_decimal(result.actual_cex_fee_usd.0),
    );
    insert_detail(
        details,
        "实际Funding成本USD",
        format_detail_decimal(result.actual_funding_cost_usd.0),
    );
    insert_detail(
        details,
        "实际边际USD",
        format_detail_decimal(result.realized_edge_usd.0),
    );
    insert_detail(
        details,
        "计划偏差USD",
        format_detail_decimal(result.plan_vs_fill_deviation_usd.0),
    );
    insert_detail(
        details,
        "实际残余Delta",
        format_detail_f64(result.actual_residual_delta),
    );
    insert_detail(
        details,
        "实际冲击亏损+1%USD",
        format_detail_decimal(result.actual_shock_loss_up_1pct.0),
    );
    insert_detail(
        details,
        "实际冲击亏损-1%USD",
        format_detail_decimal(result.actual_shock_loss_down_1pct.0),
    );
    insert_detail(
        details,
        "实际冲击亏损+2%USD",
        format_detail_decimal(result.actual_shock_loss_up_2pct.0),
    );
    insert_detail(
        details,
        "实际冲击亏损-2%USD",
        format_detail_decimal(result.actual_shock_loss_down_2pct.0),
    );
    insert_detail(details, "需要恢复", bool_zh(result.recovery_required));
    insert_detail(
        details,
        "Poly账本条数",
        result.poly_order_ledger.len().to_string(),
    );
    insert_detail(
        details,
        "CEX账本条数",
        result.cex_order_ledger.len().to_string(),
    );
    insert_detail(
        details,
        "结果时间",
        format_detail_timestamp(result.timestamp_ms),
    );
}

fn build_signal_event_details(
    candidate: &OpenCandidate,
    observed: Option<&AuditObservedMarket>,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    add_trigger_details(&mut details, &RuntimeTrigger::Candidate(candidate.clone()));
    if let Some(observed) = observed {
        add_observed_details(&mut details, observed);
    }
    maybe_details(details)
}

fn build_gate_event_details(
    candidate: &OpenCandidate,
    observed: Option<&AuditObservedMarket>,
    settings: &Settings,
    gate: &str,
    result: AuditGateResult,
    reason: &str,
    risk: Option<&InMemoryRiskManager>,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    add_trigger_details(&mut details, &RuntimeTrigger::Candidate(candidate.clone()));
    if let Some(observed) = observed {
        add_observed_details(&mut details, observed);
    }
    insert_detail(&mut details, "闸门", gate_label_zh(gate));
    insert_detail(&mut details, "闸门代码", gate.to_owned());
    insert_detail(&mut details, "闸门结果", result.label_zh());
    insert_detail(&mut details, "原因", gate_reason_label_zh(gate, reason));
    insert_detail(&mut details, "原因代码", reason.to_owned());

    match gate {
        "price_filter" => {
            let basis = effective_basis_config_for_symbol(settings, &candidate.symbol);
            insert_detail(
                &mut details,
                "价格窗口",
                price_filter_window_text_for_basis(&basis),
            );
            if let Some(observed) = observed {
                insert_detail_opt(
                    &mut details,
                    "过滤价格",
                    candidate_price_for_audit_observed(candidate, observed).map(format_detail_f64),
                );
            }
        }
        "ws_freshness" => {
            insert_detail(
                &mut details,
                "开仓 Polymarket 阈值",
                format_detail_ms(
                    settings
                        .strategy
                        .market_data
                        .resolved_poly_open_max_quote_age_ms(),
                ),
            );
            insert_detail(
                &mut details,
                "开仓 CEX 阈值",
                format_detail_ms(
                    settings
                        .strategy
                        .market_data
                        .resolved_cex_open_max_quote_age_ms(),
                ),
            );
            insert_detail(
                &mut details,
                "平仓阈值",
                format_detail_ms(
                    settings
                        .strategy
                        .market_data
                        .resolved_close_max_quote_age_ms(),
                ),
            );
            insert_detail(
                &mut details,
                "最大双腿时差",
                format_detail_ms(
                    settings
                        .strategy
                        .market_data
                        .resolved_max_cross_leg_skew_ms(),
                ),
            );
        }
        "risk_guard" => {
            if let Some(risk) = risk {
                add_risk_details(
                    &mut details,
                    risk,
                    candidate,
                    (result == AuditGateResult::Rejected).then_some(reason),
                );
            }
        }
        _ => {}
    }

    maybe_details(details)
}

fn add_planning_diagnostics_details(details: &mut EventDetails, diagnostics: &PlanningDiagnostics) {
    insert_detail(
        details,
        "Planner深度档位",
        diagnostics.planner_depth_levels.to_string(),
    );
    insert_detail_opt(
        details,
        "Poly盘口中间价",
        diagnostics
            .poly_mid_price
            .map(|price| format_detail_decimal(price.0)),
    );
    insert_detail_opt(
        details,
        "Poly最佳买价",
        diagnostics
            .poly_best_bid
            .map(|price| format_detail_decimal(price.0)),
    );
    insert_detail_opt(
        details,
        "Poly最佳卖价",
        diagnostics
            .poly_best_ask
            .map(|price| format_detail_decimal(price.0)),
    );
    insert_detail_opt(
        details,
        "Poly盘口均价",
        diagnostics
            .poly_book_average_price
            .map(|price| format_detail_decimal(price.0)),
    );
    insert_detail_opt(
        details,
        "Poly可执行价",
        diagnostics
            .poly_executable_price
            .map(|price| format_detail_decimal(price.0)),
    );
    insert_detail_opt(
        details,
        "Poly价格冲击",
        diagnostics
            .poly_price_impact_bps
            .map(|bps| format!("{} bps", format_detail_decimal(bps.round_dp(2)))),
    );
    insert_detail(
        details,
        "Poly目标股数",
        format_detail_decimal(diagnostics.poly_requested_shares.0),
    );
    insert_detail(
        details,
        "Poly计划股数",
        format_detail_decimal(diagnostics.poly_planned_shares.0),
    );
    insert_detail(
        details,
        "CEX计划数量",
        format_detail_decimal(diagnostics.cex_planned_qty.0),
    );
    insert_detail_opt(
        details,
        "计划边USD",
        diagnostics
            .planned_edge_usd
            .map(|value| format_detail_decimal(value.0)),
    );
    insert_detail_opt(
        details,
        "开仓瞬时亏损USD",
        diagnostics
            .instant_open_loss_usd
            .map(|value| format_detail_decimal(value.0)),
    );
    insert_detail_opt(
        details,
        "Poly买盘Top3",
        (!diagnostics.poly_bids_top.is_empty())
            .then(|| format_planning_book_levels(&diagnostics.poly_bids_top)),
    );
    insert_detail_opt(
        details,
        "Poly卖盘Top3",
        (!diagnostics.poly_asks_top.is_empty())
            .then(|| format_planning_book_levels(&diagnostics.poly_asks_top)),
    );
}

fn build_trade_plan_gate_event_details(
    candidate: &OpenCandidate,
    observed: Option<&AuditObservedMarket>,
    settings: &Settings,
    result: AuditGateResult,
    reason: &str,
    planning_diagnostics: Option<&PlanningDiagnostics>,
) -> Option<EventDetails> {
    let mut details = build_gate_event_details(
        candidate,
        observed,
        settings,
        "trade_plan",
        result,
        reason,
        None,
    )?;
    if let Some(diagnostics) = planning_diagnostics {
        add_planning_diagnostics_details(&mut details, diagnostics);
    }
    maybe_details(details)
}

fn format_planning_book_levels(levels: &[polyalpha_core::PlanningBookLevel]) -> String {
    levels
        .iter()
        .map(|level| {
            format!(
                "{} x {}",
                format_detail_decimal(level.price.0),
                format_detail_decimal(level.shares.0)
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn build_skip_event_details(
    reason: EvaluationSkipReason,
    warning: &EngineWarning,
    observed: Option<&AuditObservedMarket>,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "跳过原因", reason.label_zh());
    insert_detail(&mut details, "跳过原因代码", reason.key().to_owned());
    match warning {
        EngineWarning::ConnectionLost {
            symbol,
            poly_connected,
            cex_connected,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "Polymarket已连接", bool_zh(*poly_connected));
            insert_detail(&mut details, "CEX已连接", bool_zh(*cex_connected));
        }
        EngineWarning::NoCexData { symbol } | EngineWarning::NoPolyData { symbol } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
        }
        EngineWarning::CexPriceStale {
            symbol,
            cex_age_ms,
            max_age_ms,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "当前CEX报价龄", format_detail_ms(*cex_age_ms));
            insert_detail(&mut details, "CEX阈值", format_detail_ms(*max_age_ms));
        }
        EngineWarning::PolyPriceStale {
            symbol,
            poly_age_ms,
            max_age_ms,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(
                &mut details,
                "当前Polymarket报价龄",
                format_detail_ms(*poly_age_ms),
            );
            insert_detail(
                &mut details,
                "Polymarket阈值",
                format_detail_ms(*max_age_ms),
            );
        }
        EngineWarning::DataMisaligned {
            symbol,
            poly_time_ms,
            cex_time_ms,
            diff_ms,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(
                &mut details,
                "Polymarket时间",
                format_detail_timestamp(*poly_time_ms),
            );
            insert_detail(
                &mut details,
                "CEX时间",
                format_detail_timestamp(*cex_time_ms),
            );
            insert_detail(&mut details, "双腿时差", format_detail_ms(*diff_ms));
        }
    }
    if let Some(observed) = observed {
        add_observed_details(&mut details, observed);
    }
    maybe_details(details)
}

fn build_execution_event_details(
    event: &ExecutionEvent,
    trigger: Option<&RuntimeTrigger>,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    if let Some(trigger) = trigger {
        add_trigger_details(&mut details, trigger);
    }
    match event {
        ExecutionEvent::TradePlanCreated { plan } => {
            insert_detail(&mut details, "市场", plan.symbol.0.clone());
            insert_detail(&mut details, "计划ID", plan.plan_id.clone());
            insert_detail(&mut details, "意图", plan.intent_type.clone());
            insert_detail(&mut details, "优先级", plan.priority.clone());
            insert_detail(&mut details, "关联ID", plan.correlation_id.clone());
            add_trade_plan_details(&mut details, plan);
        }
        ExecutionEvent::PlanSuperseded {
            symbol,
            superseded_plan_id,
            next_plan_id,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "被替换计划", superseded_plan_id.clone());
            insert_detail(&mut details, "新计划", next_plan_id.clone());
        }
        ExecutionEvent::RecoveryPlanCreated { plan } => {
            insert_detail(&mut details, "市场", plan.symbol.0.clone());
            insert_detail(&mut details, "恢复计划ID", plan.plan_id.clone());
            insert_detail(&mut details, "关联ID", plan.correlation_id.clone());
            insert_detail(&mut details, "优先级", plan.priority.clone());
            add_trade_plan_details(&mut details, plan);
        }
        ExecutionEvent::ExecutionResultRecorded { result } => {
            insert_detail(&mut details, "市场", result.symbol.0.clone());
            insert_detail(&mut details, "计划ID", result.plan_id.clone());
            insert_detail(&mut details, "关联ID", result.correlation_id.clone());
            add_execution_result_details(&mut details, result);
        }
        ExecutionEvent::OrderSubmitted {
            symbol,
            exchange,
            response,
            correlation_id,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "交易所", format_exchange_label(*exchange));
            insert_detail(&mut details, "订单ID", response.exchange_order_id.0.clone());
            insert_detail(
                &mut details,
                "订单状态",
                display_order_status_text(&format!("{:?}", response.status)),
            );
            insert_detail(&mut details, "关联ID", correlation_id.clone());
            insert_detail(
                &mut details,
                "时间",
                format_detail_timestamp(response.timestamp_ms),
            );
            insert_detail_opt(
                &mut details,
                "成交均价",
                response
                    .average_price
                    .as_ref()
                    .and_then(|price| price.0.to_f64())
                    .map(format_detail_f64),
            );
            insert_detail_opt(
                &mut details,
                "已成交数量",
                Some(match &response.filled_quantity {
                    VenueQuantity::PolyShares(shares) => format_detail_decimal(shares.0),
                    VenueQuantity::CexBaseQty(qty) => format_detail_decimal(qty.0),
                }),
            );
            insert_detail_opt(&mut details, "拒绝原因", response.rejection_reason.clone());
        }
        ExecutionEvent::OrderFilled(fill) => {
            insert_detail(&mut details, "市场", fill.symbol.0.clone());
            insert_detail(&mut details, "交易所", format_exchange_label(fill.exchange));
            insert_detail(&mut details, "方向", format_order_side_label(fill.side));
            insert_detail(&mut details, "价格", format_detail_decimal(fill.price.0));
            insert_detail(
                &mut details,
                "数量",
                match &fill.quantity {
                    VenueQuantity::PolyShares(shares) => format_detail_decimal(shares.0),
                    VenueQuantity::CexBaseQty(qty) => format_detail_decimal(qty.0),
                },
            );
            insert_detail(
                &mut details,
                "名义",
                format_detail_decimal(fill.notional_usd.0),
            );
            insert_detail(&mut details, "手续费", format_detail_decimal(fill.fee.0));
            insert_detail(&mut details, "Maker", bool_zh(fill.is_maker));
            insert_detail(&mut details, "订单ID", fill.order_id.0.clone());
            insert_detail(&mut details, "成交ID", fill.fill_id.clone());
            insert_detail(&mut details, "关联ID", fill.correlation_id.clone());
            insert_detail(
                &mut details,
                "时间",
                format_detail_timestamp(fill.timestamp_ms),
            );
        }
        ExecutionEvent::OrderCancelled {
            symbol,
            order_id,
            exchange,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "交易所", format_exchange_label(*exchange));
            insert_detail(&mut details, "订单ID", order_id.0.clone());
        }
        ExecutionEvent::HedgeStateChanged {
            symbol,
            old_state,
            new_state,
            timestamp_ms,
            ..
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "旧状态", format_hedge_state_label(*old_state));
            insert_detail(&mut details, "新状态", format_hedge_state_label(*new_state));
            insert_detail(&mut details, "时间", format_detail_timestamp(*timestamp_ms));
        }
        ExecutionEvent::ReconcileRequired { symbol, reason } => {
            insert_detail_opt(
                &mut details,
                "市场",
                symbol.as_ref().map(|symbol| symbol.0.clone()),
            );
            insert_detail(&mut details, "原因", reason.clone());
        }
        ExecutionEvent::TradeClosed {
            symbol,
            correlation_id,
            realized_pnl,
            timestamp_ms,
        } => {
            insert_detail(&mut details, "市场", symbol.0.clone());
            insert_detail(&mut details, "关联ID", correlation_id.clone());
            insert_detail(
                &mut details,
                "已实现盈亏",
                format_detail_decimal(realized_pnl.0),
            );
            insert_detail(&mut details, "时间", format_detail_timestamp(*timestamp_ms));
        }
    }
    maybe_details(details)
}

fn build_post_submit_rejection_details(
    candidate: &OpenCandidate,
    rejection_reasons: &[String],
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    add_trigger_details(&mut details, &RuntimeTrigger::Candidate(candidate.clone()));
    insert_detail(&mut details, "闸门", "执行层");
    insert_detail(&mut details, "闸门结果", "拒绝");
    insert_detail(&mut details, "原因", rejection_reasons.join("，"));
    insert_detail_opt(
        &mut details,
        "计划拒绝代码",
        rejection_reasons
            .iter()
            .find_map(|reason| extract_plan_rejection_reason(reason)),
    );
    maybe_details(details)
}

fn build_intent_rejection_details(
    trigger: &RuntimeTrigger,
    reason_code: &str,
    message: &str,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    add_trigger_details(&mut details, trigger);
    insert_detail(&mut details, "闸门", gate_label_zh("trade_plan"));
    insert_detail(&mut details, "闸门结果", "拒绝");
    insert_detail(
        &mut details,
        "原因",
        translate_trade_plan_rejection(reason_code),
    );
    insert_detail(&mut details, "计划拒绝代码", reason_code.to_owned());
    insert_detail(&mut details, "原始错误", message.to_owned());
    maybe_details(details)
}

fn build_command_event_details(
    command_id: &str,
    kind: &CommandKind,
    status: Option<CommandStatus>,
    message: Option<&str>,
    error_code: Option<u32>,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "命令ID", command_id.to_owned());
    insert_detail(&mut details, "命令", describe_command_kind_zh(kind));
    if let Some(status) = status {
        insert_detail(&mut details, "状态", format_command_status_label(status));
    }
    if let Some(message) = message {
        insert_detail(&mut details, "消息", message.to_owned());
    }
    if let Some(error_code) = error_code {
        insert_detail(&mut details, "错误码", error_code.to_string());
    }
    if let CommandKind::CancelCommand { command_id } = kind {
        insert_detail(&mut details, "目标命令", command_id.clone());
    }
    maybe_details(details)
}

fn build_connection_event_details(
    exchange_name: &str,
    status: ConnectionStatus,
    info: Option<&ConnectionInfo>,
    affected_markets: usize,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    insert_detail(
        &mut details,
        "连接",
        display_exchange_text(exchange_name).to_owned(),
    );
    insert_detail(
        &mut details,
        "连接状态",
        format_connection_status_label(status),
    );
    insert_detail(&mut details, "影响市场数", affected_markets.to_string());
    if let Some(info) = info {
        insert_detail_opt(
            &mut details,
            "更新时间",
            info.updated_at_ms.map(format_detail_timestamp),
        );
        insert_detail_opt(
            &mut details,
            "空闲时长",
            info.transport_idle_ms.map(format_detail_ms),
        );
        insert_detail(&mut details, "重连次数", info.reconnect_count.to_string());
        insert_detail(&mut details, "断开次数", info.disconnect_count.to_string());
        insert_detail_opt(
            &mut details,
            "最近断开",
            info.last_disconnect_at_ms.map(format_detail_timestamp),
        );
    }
    maybe_details(details)
}

fn build_market_phase_event_details(
    symbol: &Symbol,
    phase: &MarketPhase,
    timestamp_ms: u64,
) -> Option<EventDetails> {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "市场", symbol.0.clone());
    insert_detail(&mut details, "市场阶段", format_market_phase_label(phase));
    insert_detail(&mut details, "时间", format_detail_timestamp(timestamp_ms));
    maybe_details(details)
}

fn push_event_with_context_and_payload(
    events: &mut VecDeque<PaperEventLog>,
    kind: &str,
    summary: String,
    market: Option<String>,
    signal_id: Option<String>,
    correlation_id: Option<String>,
    details: Option<EventDetails>,
    payload: Option<MonitorEventPayload>,
) {
    insert_recent_event(
        events,
        PaperEventLog {
            seq: next_recent_event_seq(events),
            timestamp_ms: now_millis(),
            kind: kind.to_owned(),
            summary,
            market,
            signal_id,
            correlation_id,
            details,
            payload,
        },
    );
}

fn push_event_with_context(
    events: &mut VecDeque<PaperEventLog>,
    kind: &str,
    summary: String,
    market: Option<String>,
    signal_id: Option<String>,
    correlation_id: Option<String>,
    details: Option<EventDetails>,
) {
    push_event_with_context_and_payload(
        events,
        kind,
        summary,
        market,
        signal_id,
        correlation_id,
        details,
        None,
    );
}

fn next_recent_event_seq(events: &VecDeque<PaperEventLog>) -> u64 {
    events.back().map(|event| event.seq + 1).unwrap_or(1)
}

fn push_signal_event(
    events: &mut VecDeque<PaperEventLog>,
    kind: &str,
    summary: String,
    candidate: &OpenCandidate,
    details: Option<EventDetails>,
) {
    push_event_with_context_and_payload(
        events,
        kind,
        summary,
        Some(candidate.symbol.0.clone()),
        Some(candidate.candidate_id.clone()),
        Some(candidate.correlation_id.clone()),
        details,
        Some(candidate_monitor_payload(candidate)),
    );
}

fn push_gate_event(
    events: &mut VecDeque<PaperEventLog>,
    candidate: &OpenCandidate,
    summary: String,
    details: Option<EventDetails>,
) {
    push_signal_event(events, "gate", summary, candidate, details);
}

fn push_event(
    events: &mut VecDeque<PaperEventLog>,
    kind: &str,
    summary: String,
    correlation_id: Option<String>,
) {
    push_event_with_context(events, kind, summary, None, None, correlation_id, None);
}

fn push_event_simple(events: &mut VecDeque<PaperEventLog>, kind: &str, summary: String) {
    push_event(events, kind, summary, None);
}

fn handle_funding_refresh_result(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    result: FundingRefreshResult,
) {
    if result.failed_targets == 0 {
        return;
    }

    stats.poll_errors += result.poll_errors;
    push_event_simple(
        recent_events,
        "poll_error",
        format!(
            "后台资金费刷新完成：成功 {}/{}，失败 {}，耗时 {:.1}s",
            result.total_targets.saturating_sub(result.failed_targets),
            result.total_targets,
            result.failed_targets,
            result.duration_ms as f64 / 1000.0,
        ),
    );

    let detail_limit = 3usize;
    for error in result.target_errors.iter().take(detail_limit) {
        push_event_simple(recent_events, "poll_error", error.clone());
    }
    if result.target_errors.len() > detail_limit {
        push_event_simple(
            recent_events,
            "poll_error",
            format!(
                "后台资金费刷新还有 {} 条失败详情未展示",
                result.target_errors.len() - detail_limit
            ),
        );
    }
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
        "轮次={} 市场={} 阶段={} 连接={} Polymarket-YES={:?} Polymarket-NO={:?} 交易所中价={:?} 信号腿={:?} 基差={:?} Z值={:?} 公允值={:?} Delta={:?} 交易所参考价={:?} 对冲={} 最近信号={:?} 挂单={} DMM挂单={} 信号={}/{} 提交={} 撤单={} 成交={} 轮询错误={} 持仓={} 敞口={} 盈亏={} 熔断={}",
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
        snapshot.poll_errors,
        snapshot.positions.len(),
        snapshot.total_exposure_usd,
        snapshot.daily_pnl_usd,
        display_circuit_breaker_text(&snapshot.breaker),
    );
}

fn print_artifact_table(artifact: &PaperArtifact, artifact_path: &PathBuf) {
    print_report_title(&format!(
        "{}会话回看",
        artifact_runtime_label(artifact.trading_mode)
    ));

    print_report_section("会话概览");
    print_report_kv("结果文件", artifact_path.display().to_string());
    print_report_kv("运行模式", artifact_runtime_label(artifact.trading_mode));
    print_report_kv("环境", artifact.env.as_str());
    print_report_kv("市场", artifact.market.as_str());
    print_report_kv(
        "对冲交易所",
        display_exchange_text(artifact.hedge_exchange.as_str()),
    );
    print_report_kv(
        "行情模式",
        display_market_data_mode_text(artifact.market_data_mode.as_deref().unwrap_or("poll")),
    );
    print_report_kv("轮询间隔", format!("{} ms", artifact.poll_interval_ms));
    print_report_kv("订单簿深度", artifact.depth.to_string());
    print_report_kv(
        "轮询资金费/标记价",
        if artifact.include_funding {
            "是"
        } else {
            "否"
        },
    );
    print_report_kv("已处理轮次", artifact.ticks_processed.to_string());
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

fn sorted_connections(connections: &HashMap<String, ConnectionStatus>) -> Vec<String> {
    let mut items = connections
        .iter()
        .map(|(exchange, status)| format!("{exchange}={}", format_connection_status(*status)))
        .collect::<Vec<_>>();
    items.sort();
    items
}

fn format_connection_status(status: ConnectionStatus) -> &'static str {
    match status {
        ConnectionStatus::Connecting => "Connecting",
        ConnectionStatus::Connected => "Connected",
        ConnectionStatus::Disconnected => "Disconnected",
        ConnectionStatus::Reconnecting => "Reconnecting",
    }
}

fn artifact_runtime_dir(trading_mode: polyalpha_core::TradingMode) -> &'static str {
    match trading_mode {
        polyalpha_core::TradingMode::Paper => "paper",
        polyalpha_core::TradingMode::Live => "live",
        polyalpha_core::TradingMode::Backtest => "backtest",
    }
}

fn artifact_runtime_label(trading_mode: polyalpha_core::TradingMode) -> &'static str {
    match trading_mode {
        polyalpha_core::TradingMode::Paper => "纸面盘",
        polyalpha_core::TradingMode::Live => "实盘",
        polyalpha_core::TradingMode::Backtest => "回测",
    }
}

fn runtime_initial_capital(settings: &Settings, trading_mode: polyalpha_core::TradingMode) -> f64 {
    match trading_mode {
        polyalpha_core::TradingMode::Paper => settings.paper.initial_capital,
        polyalpha_core::TradingMode::Live => 0.0,
        polyalpha_core::TradingMode::Backtest => settings.paper.initial_capital,
    }
}

fn artifact_path(
    settings: &Settings,
    env: &str,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<PathBuf> {
    let mut path = PathBuf::from(&settings.general.data_dir);
    path.push(artifact_runtime_dir(trading_mode));
    fs::create_dir_all(&path).with_context(|| {
        format!(
            "创建{}结果目录 `{}` 失败",
            artifact_runtime_label(trading_mode),
            path.display()
        )
    })?;
    path.push(format!("{env}-last-run.json"));
    Ok(path)
}

fn resolve_artifact_for_read(
    settings: &Settings,
    env: &str,
    trading_mode: polyalpha_core::TradingMode,
) -> Result<PathBuf> {
    let current = artifact_path(settings, env, trading_mode)?;
    if current.exists() {
        return Ok(current);
    }

    if trading_mode == polyalpha_core::TradingMode::Live {
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
    fs::write(path, payload).with_context(|| format!("写入纸面盘结果 `{}` 失败", path.display()))
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

fn audit_counter_snapshot(stats: &PaperStats) -> AuditCounterSnapshot {
    let ws_diagnostics = stats.polymarket_ws_diagnostics.snapshot();
    AuditCounterSnapshot {
        ticks_processed: stats.ticks_processed as u64,
        market_events: stats.market_events as u64,
        signals_seen: stats.signals_seen as u64,
        signals_rejected: stats.signals_rejected as u64,
        evaluation_attempts: stats.evaluation_attempts,
        evaluable_passes: stats.evaluable_passes,
        evaluation_skipped: stats.evaluation_skipped,
        execution_rejected: stats.execution_rejected,
        order_submitted: stats.order_submitted as u64,
        order_cancelled: stats.order_cancelled as u64,
        fills: stats.fills as u64,
        state_changes: stats.state_changes as u64,
        poll_errors: stats.poll_errors as u64,
        snapshot_resync_count: stats.snapshot_resync_count as u64,
        funding_refresh_count: stats.funding_refresh_count as u64,
        trades_closed: stats.trades_closed as u64,
        market_data_rx_lag_events: stats.market_data_rx_lag_events,
        market_data_rx_lagged_messages: stats.market_data_rx_lagged_messages,
        market_data_tick_drain_last: stats.market_data_tick_drain_last,
        market_data_tick_drain_max: stats.market_data_tick_drain_max,
        polymarket_ws_text_frames: ws_diagnostics.text_frames,
        polymarket_ws_price_change_messages: ws_diagnostics.price_change_messages,
        polymarket_ws_book_messages: ws_diagnostics.book_messages,
        polymarket_ws_orderbook_updates: ws_diagnostics.orderbook_updates,
    }
}

fn build_audit_checkpoint(
    session_id: &str,
    stats: &PaperStats,
    monitor_state: &MonitorState,
    tick_index: usize,
    now_ms: u64,
) -> AuditCheckpoint {
    AuditCheckpoint {
        session_id: session_id.to_owned(),
        updated_at_ms: now_ms,
        tick_index: tick_index as u64,
        counters: audit_counter_snapshot(stats),
        monitor_state: monitor_state.clone(),
    }
}

fn warning_symbol_opt(warning: Option<&EngineWarning>) -> Option<&Symbol> {
    warning.map(warning_symbol)
}

fn audit_execution_identity(
    event: &ExecutionEvent,
    trigger: Option<&RuntimeTrigger>,
) -> (Option<String>, Option<String>) {
    match event {
        ExecutionEvent::TradePlanCreated { plan } => (
            Some(plan.symbol.0.clone()),
            Some(plan.correlation_id.clone()),
        ),
        ExecutionEvent::PlanSuperseded { symbol, .. } => (
            Some(symbol.0.clone()),
            trigger.map(|trigger| trigger.correlation_id().to_owned()),
        ),
        ExecutionEvent::RecoveryPlanCreated { plan } => (
            Some(plan.symbol.0.clone()),
            Some(plan.correlation_id.clone()),
        ),
        ExecutionEvent::ExecutionResultRecorded { result } => (
            Some(result.symbol.0.clone()),
            Some(result.correlation_id.clone()),
        ),
        ExecutionEvent::OrderSubmitted {
            symbol,
            correlation_id,
            ..
        } => (Some(symbol.0.clone()), Some(correlation_id.clone())),
        ExecutionEvent::OrderFilled(fill) => (
            Some(fill.symbol.0.clone()),
            trigger.map(|trigger| trigger.correlation_id().to_owned()),
        ),
        ExecutionEvent::OrderCancelled { symbol, .. } => (
            Some(symbol.0.clone()),
            trigger.map(|trigger| trigger.correlation_id().to_owned()),
        ),
        ExecutionEvent::HedgeStateChanged { symbol, .. } => (
            Some(symbol.0.clone()),
            trigger.map(|trigger| trigger.correlation_id().to_owned()),
        ),
        ExecutionEvent::ReconcileRequired { symbol, .. } => (
            symbol.as_ref().map(|symbol| symbol.0.clone()),
            trigger.map(|trigger| trigger.correlation_id().to_owned()),
        ),
        ExecutionEvent::TradeClosed {
            symbol,
            correlation_id,
            ..
        } => (Some(symbol.0.clone()), Some(correlation_id.clone())),
    }
}

fn audit_execution_timestamp_ms(event: &ExecutionEvent) -> u64 {
    match event {
        ExecutionEvent::TradePlanCreated { plan } => plan.created_at_ms,
        ExecutionEvent::PlanSuperseded { .. } => now_millis(),
        ExecutionEvent::RecoveryPlanCreated { plan } => plan.created_at_ms,
        ExecutionEvent::ExecutionResultRecorded { result } => result.timestamp_ms,
        ExecutionEvent::OrderSubmitted { response, .. } => response.timestamp_ms,
        ExecutionEvent::OrderFilled(fill) => fill.timestamp_ms,
        ExecutionEvent::OrderCancelled { .. } => now_millis(),
        ExecutionEvent::HedgeStateChanged { timestamp_ms, .. } => *timestamp_ms,
        ExecutionEvent::ReconcileRequired { .. } => now_millis(),
        ExecutionEvent::TradeClosed { timestamp_ms, .. } => *timestamp_ms,
    }
}

fn position_exposure_notional(position: &polyalpha_core::PositionView) -> f64 {
    (position.poly_current_price * position.poly_quantity.abs())
        + (position.cex_current_price * position.cex_quantity.abs())
}

#[derive(Default)]
struct GroupedPosition {
    poly_yes: Option<Position>,
    poly_no: Option<Position>,
    cex: Option<Position>,
}

fn build_monitor_state(
    settings: &Settings,
    market: &MarketConfig,
    engine: &SimpleAlphaEngine,
    observed: &ObservedState,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    build_single_monitor_state_from_seed_cache(
        settings,
        market,
        engine,
        observed,
        stats,
        risk,
        execution,
        trade_book,
        command_center,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        recent_events,
        trading_mode,
    )
}

fn group_positions_by_symbol(
    risk: &InMemoryRiskManager,
    now_ms: u64,
) -> HashMap<Symbol, GroupedPosition> {
    let snapshot = risk.build_snapshot(now_ms);
    let mut grouped: HashMap<Symbol, GroupedPosition> = HashMap::new();

    for position in snapshot.positions.into_values() {
        let quantity = quantity_to_f64(position.quantity);
        if quantity <= f64::EPSILON {
            continue;
        }

        let entry = grouped.entry(position.key.symbol.clone()).or_default();
        match position.key.instrument {
            InstrumentKind::PolyYes => entry.poly_yes = Some(position),
            InstrumentKind::PolyNo => entry.poly_no = Some(position),
            InstrumentKind::CexPerp => entry.cex = Some(position),
        }
    }

    grouped
}

fn group_positions_from_snapshot(
    snapshot: &polyalpha_core::RiskStateSnapshot,
) -> HashMap<Symbol, GroupedPosition> {
    let mut grouped: HashMap<Symbol, GroupedPosition> = HashMap::new();

    for position in snapshot.positions.values() {
        let quantity = quantity_to_f64(position.quantity);
        if quantity <= f64::EPSILON {
            continue;
        }

        let entry = grouped.entry(position.key.symbol.clone()).or_default();
        match position.key.instrument {
            InstrumentKind::PolyYes => entry.poly_yes = Some(position.clone()),
            InstrumentKind::PolyNo => entry.poly_no = Some(position.clone()),
            InstrumentKind::CexPerp => entry.cex = Some(position.clone()),
        }
    }

    grouped
}

#[derive(Clone, Debug, Default)]
struct MonitorMarketSeed {
    observed: ObservedState,
    basis_snapshot: Option<BasisStrategySnapshot>,
    basis_history_len: usize,
    has_position: bool,
    active_plan_priority: Option<String>,
}

#[derive(Clone)]
struct MultiMonitorRenderRequest {
    settings: Settings,
    stats: PaperStats,
    performance: PerformanceMetrics,
    position_views: Vec<polyalpha_core::PositionView>,
    recent_events: Vec<MonitorEvent>,
    recent_trades: Vec<TradeView>,
    recent_commands: Vec<CommandView>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    trading_mode: polyalpha_core::TradingMode,
    warmup_status_by_symbol: HashMap<Symbol, MarketWarmupStatus>,
    min_warmup_samples: usize,
    warmup_total_markets: usize,
    warmup_completed_markets: usize,
    warmup_failed_markets: usize,
    has_monitor_clients: bool,
    emit_strategy_readiness_alert: bool,
    tick_index: usize,
    apply_strategy_health: bool,
}

#[derive(Clone)]
struct MultiMonitorRenderResult {
    monitor_state: MonitorState,
    stats: PaperStats,
    tick_index: usize,
    now_ms: u64,
    has_monitor_clients: bool,
    readiness_alert: Option<StrategyReadinessAlert>,
}

fn build_monitor_market_seed_from_observed(
    symbol: &Symbol,
    observed: &ObservedState,
    engine: &SimpleAlphaEngine,
    execution: &ExecutionManager<RuntimeExecutor>,
    risk: &InMemoryRiskManager,
) -> MonitorMarketSeed {
    let basis_snapshot = engine.basis_snapshot(symbol);
    let (yes_basis_history_len, no_basis_history_len) = engine.basis_history_lengths(symbol);

    MonitorMarketSeed {
        observed: observed.clone(),
        basis_snapshot,
        basis_history_len: yes_basis_history_len.max(no_basis_history_len),
        has_position: risk.position_tracker().symbol_has_open_position(symbol),
        active_plan_priority: execution.active_plan_priority(symbol).map(str::to_owned),
    }
}

fn build_monitor_market_seed(
    symbol: &Symbol,
    observed_map: &HashMap<Symbol, ObservedState>,
    engine: &SimpleAlphaEngine,
    execution: &ExecutionManager<RuntimeExecutor>,
    risk: &InMemoryRiskManager,
) -> MonitorMarketSeed {
    let observed = observed_map.get(symbol).cloned().unwrap_or_default();
    build_monitor_market_seed_from_observed(symbol, &observed, engine, execution, risk)
}

fn refresh_multi_monitor_cache(
    cache: &Arc<std::sync::RwLock<HashMap<Symbol, MonitorMarketSeed>>>,
    symbols: &[Symbol],
    observed_map: &HashMap<Symbol, ObservedState>,
    engine: &SimpleAlphaEngine,
    execution: &ExecutionManager<RuntimeExecutor>,
    risk: &InMemoryRiskManager,
) {
    if let Ok(mut guard) = cache.write() {
        for symbol in symbols {
            guard.insert(
                symbol.clone(),
                build_monitor_market_seed(symbol, observed_map, engine, execution, risk),
            );
        }
    }
}

fn refresh_single_monitor_cache(
    cache: &Arc<std::sync::RwLock<HashMap<Symbol, MonitorMarketSeed>>>,
    symbol: &Symbol,
    observed: &ObservedState,
    engine: &SimpleAlphaEngine,
    execution: &ExecutionManager<RuntimeExecutor>,
    risk: &InMemoryRiskManager,
) {
    if let Ok(mut guard) = cache.write() {
        guard.insert(
            symbol.clone(),
            build_monitor_market_seed_from_observed(symbol, observed, engine, execution, risk),
        );
    }
}

fn build_market_view_from_seed(
    settings: &Settings,
    market: &MarketConfig,
    seed: Option<&MonitorMarketSeed>,
    now_ms: u64,
) -> MarketView {
    let basis = effective_basis_config_for_market(settings, market);
    let thresholds = freshness_thresholds(settings, &basis);
    let require_connections = require_connected_inputs(&basis);
    let observed = seed.map(|seed| &seed.observed);
    let snapshot = seed.and_then(|seed| seed.basis_snapshot.as_ref());
    let active_token_side_value = snapshot.map(|item| item.token_side);
    let active_token_side = snapshot
        .map(|item| format!("{:?}", item.token_side).to_ascii_uppercase());
    let has_position = seed.map(|item| item.has_position).unwrap_or(false);
    let poly_yes_price = observed.and_then(|item| item.poly_yes_mid.and_then(|value| value.to_f64()));
    let poly_no_price = observed.and_then(|item| item.poly_no_mid.and_then(|value| value.to_f64()));
    let poly_price = match snapshot.map(|item| item.token_side) {
        Some(TokenSide::Yes) => poly_yes_price.or(poly_no_price),
        Some(TokenSide::No) => poly_no_price.or(poly_yes_price),
        None => poly_yes_price.or(poly_no_price),
    };
    let async_snapshot = observed.map(|item| {
        classify_async_market(
            now_ms,
            market,
            item,
            thresholds,
            active_token_side_value,
            false,
            require_connections,
        )
    });
    let retention_reason = market_retention_reason(
        has_position,
        seed.and_then(|item| item.active_plan_priority.as_deref()),
    );
    let near_opportunity = market_is_near_opportunity(settings, snapshot);
    let semantics_guard_active =
        market_open_semantics_guard_reason(market, &settings.strategy.market_scope, now_ms)
            .is_some();
    let market_tier = determine_market_tier(
        settings,
        !semantics_guard_active
            && async_snapshot
                .as_ref()
                .is_some_and(async_snapshot_is_tradeable),
        retention_reason,
        !semantics_guard_active && near_opportunity,
        observed.and_then(|item| item.market_pool.last_focus_at_ms),
        observed.and_then(|item| item.market_pool.last_tradeable_at_ms),
        observed
            .and_then(|item| market_pool_cooldown_reason(item, now_ms))
            .is_some(),
        now_ms,
    );

    MarketView {
        symbol: market.symbol.0.clone(),
        z_score: snapshot.and_then(|item| item.z_score),
        basis_pct: snapshot.map(|item| item.signal_basis * 100.0),
        poly_price,
        poly_yes_price,
        poly_no_price,
        cex_price: observed.and_then(|item| item.cex_mid.and_then(|value| value.to_f64())),
        fair_value: snapshot.map(|item| item.fair_value),
        has_position,
        minutes_to_expiry: snapshot.map(|item| item.minutes_to_expiry),
        basis_history_len: seed.map(|item| item.basis_history_len).unwrap_or_default(),
        raw_sigma: snapshot.and_then(|item| item.raw_sigma),
        effective_sigma: snapshot.and_then(|item| item.sigma),
        sigma_source: snapshot.map(|item| item.sigma_source.clone()),
        returns_window_len: snapshot
            .map(|item| item.returns_window_len)
            .unwrap_or_default(),
        active_token_side,
        poly_updated_at_ms: async_snapshot
            .as_ref()
            .and_then(|item| item.poly_updated_at_ms),
        cex_updated_at_ms: async_snapshot
            .as_ref()
            .and_then(|item| item.cex_updated_at_ms),
        poly_quote_age_ms: async_snapshot
            .as_ref()
            .and_then(|item| item.poly_quote_age_ms),
        cex_quote_age_ms: async_snapshot
            .as_ref()
            .and_then(|item| item.cex_quote_age_ms),
        cross_leg_skew_ms: async_snapshot
            .as_ref()
            .and_then(|item| item.cross_leg_skew_ms),
        evaluable_status: async_snapshot
            .as_ref()
            .map(|item| item.evaluable_status)
            .unwrap_or_default(),
        async_classification: async_snapshot
            .as_ref()
            .map(|item| item.async_classification)
            .unwrap_or_default(),
        market_tier,
        retention_reason,
        last_focus_at_ms: observed.and_then(|item| item.market_pool.last_focus_at_ms),
        last_tradeable_at_ms: observed.and_then(|item| item.market_pool.last_tradeable_at_ms),
    }
}

fn aggregate_connections_from_seed_cache(
    market_seeds: &HashMap<Symbol, MonitorMarketSeed>,
    now_ms: u64,
) -> HashMap<String, ConnectionInfo> {
    let mut out = HashMap::new();
    for seed in market_seeds.values() {
        for (name, status) in &seed.observed.connections {
            let updated_at_ms = seed.observed.connection_updated_at_ms.get(name).copied();
            let reconnect_count = seed
                .observed
                .connection_reconnect_count
                .get(name)
                .copied()
                .unwrap_or_default();
            let disconnect_count = seed
                .observed
                .connection_disconnect_count
                .get(name)
                .copied()
                .unwrap_or_default();
            let last_disconnect_at_ms = seed
                .observed
                .connection_last_disconnect_at_ms
                .get(name)
                .copied();

            out.entry(name.clone())
                .and_modify(|info: &mut ConnectionInfo| {
                    info.status = *status;
                    if updated_at_ms.unwrap_or_default() > info.updated_at_ms.unwrap_or_default() {
                        info.updated_at_ms = updated_at_ms;
                    }
                    info.reconnect_count = info.reconnect_count.max(reconnect_count);
                    info.disconnect_count = info.disconnect_count.max(disconnect_count);
                    if last_disconnect_at_ms.unwrap_or_default()
                        > info.last_disconnect_at_ms.unwrap_or_default()
                    {
                        info.last_disconnect_at_ms = last_disconnect_at_ms;
                    }
                })
                .or_insert(ConnectionInfo {
                    status: *status,
                    latency_ms: None,
                    updated_at_ms,
                    transport_idle_ms: updated_at_ms.map(|ts| now_ms.saturating_sub(ts)),
                    reconnect_count,
                    disconnect_count,
                    last_disconnect_at_ms,
                });
        }
    }

    for info in out.values_mut() {
        info.transport_idle_ms = info.updated_at_ms.map(|ts| now_ms.saturating_sub(ts));
    }

    out
}

fn build_performance_metrics(
    settings: &Settings,
    trading_mode: polyalpha_core::TradingMode,
    stats: &PaperStats,
    risk_snapshot: &polyalpha_core::RiskStateSnapshot,
    position_views: &[polyalpha_core::PositionView],
    trade_book: &mut TradeBook,
    now_ms: u64,
) -> PerformanceMetrics {
    let open_unrealized_pnl: f64 = position_views
        .iter()
        .map(|position| position.total_pnl_usd)
        .sum();
    let realized_pnl_usd: f64 = risk_snapshot
        .positions
        .values()
        .map(|position| position.realized_pnl.0.to_f64().unwrap_or_default())
        .sum();
    let total_pnl_usd = stats.total_pnl_usd + realized_pnl_usd + open_unrealized_pnl;
    let initial_capital = runtime_initial_capital(settings, trading_mode);
    let equity = initial_capital + total_pnl_usd;
    trade_book.push_equity(now_ms, equity);

    PerformanceMetrics {
        total_pnl_usd,
        today_pnl_usd: trade_book.today_pnl_usd(equity, initial_capital, now_ms),
        win_rate_pct: trade_book.win_rate_pct(),
        max_drawdown_pct: trade_book.max_drawdown_pct(),
        profit_factor: trade_book.profit_factor(),
        equity,
        initial_capital,
    }
}

fn build_monitor_render_request(
    settings: &Settings,
    stats: &PaperStats,
    observed_map: &HashMap<Symbol, ObservedState>,
    engine: &SimpleAlphaEngine,
    risk: &InMemoryRiskManager,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
    warmup_status_by_symbol: HashMap<Symbol, MarketWarmupStatus>,
    min_warmup_samples: usize,
    warmup_total_markets: usize,
    warmup_completed_markets: usize,
    warmup_failed_markets: usize,
    has_monitor_clients: bool,
    emit_strategy_readiness_alert: bool,
    tick_index: usize,
    apply_strategy_health: bool,
) -> MultiMonitorRenderRequest {
    let risk_snapshot = risk.build_snapshot(now_ms);
    let grouped_positions = group_positions_from_snapshot(&risk_snapshot);
    let position_views = build_position_views(
        settings,
        &grouped_positions,
        observed_map,
        engine,
        trade_book,
        now_ms,
    );
    let performance = build_performance_metrics(
        settings,
        trading_mode,
        stats,
        &risk_snapshot,
        &position_views,
        trade_book,
        now_ms,
    );
    let recent_commands = command_center
        .lock()
        .map(|center| center.recent_commands())
        .unwrap_or_default();

    MultiMonitorRenderRequest {
        settings: settings.clone(),
        stats: stats.clone(),
        performance,
        position_views,
        recent_events: recent_events.to_vec(),
        recent_trades: trade_book.recent_trades(),
        recent_commands,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        trading_mode,
        warmup_status_by_symbol,
        min_warmup_samples,
        warmup_total_markets,
        warmup_completed_markets,
        warmup_failed_markets,
        has_monitor_clients,
        emit_strategy_readiness_alert,
        tick_index,
        apply_strategy_health,
    }
}

fn monitor_cache_symbols_for_market_event(
    event: &MarketDataEvent,
    registry: &SymbolRegistry,
    all_symbols: &[Symbol],
) -> Vec<Symbol> {
    match event {
        MarketDataEvent::ConnectionEvent { .. } => all_symbols.to_vec(),
        _ => market_symbols_for_event(event, registry),
    }
}

fn build_multi_monitor_state_from_seed_cache(
    request: &MultiMonitorRenderRequest,
    market_seeds: &HashMap<Symbol, MonitorMarketSeed>,
) -> MultiMonitorRenderResult {
    let market_views = request
        .settings
        .markets
        .iter()
        .map(|market| {
            build_market_view_from_seed(
                &request.settings,
                market,
                market_seeds.get(&market.symbol),
                request.now_ms,
            )
        })
        .collect::<Vec<_>>();

    let mut monitor_state = MonitorState {
        schema_version: PLANNING_SCHEMA_VERSION,
        timestamp_ms: request.now_ms,
        mode: request.trading_mode,
        uptime_secs: (request.now_ms.saturating_sub(request.start_time_ms)) / 1000,
        paused: request.paused,
        emergency: request.emergency,
        performance: request.performance.clone(),
        positions: request.position_views.clone(),
        markets: market_views,
        signals: polyalpha_core::SignalStats {
            seen: request.stats.signals_seen as u64,
            rejected: request.stats.signals_rejected as u64,
            rejection_reasons: request.stats.rejection_reasons.clone(),
        },
        evaluation: EvaluationStats {
            attempts: request.stats.evaluation_attempts,
            evaluable_passes: request.stats.evaluable_passes,
            skipped: request.stats.evaluation_skipped,
            execution_rejected: request.stats.execution_rejected,
            skip_reasons: request.stats.skip_reasons.clone(),
            evaluable_status_counts: request.stats.evaluable_status_counts.clone(),
            async_classification_counts: request.stats.async_classification_counts.clone(),
        },
        config: build_monitor_strategy_config(&request.settings, &request.settings.markets),
        runtime: MonitorRuntimeStats {
            snapshot_resync_count: request.stats.snapshot_resync_count as u64,
            funding_refresh_count: request.stats.funding_refresh_count as u64,
            market_data_rx_lag_events: request.stats.market_data_rx_lag_events,
            market_data_rx_lagged_messages: request.stats.market_data_rx_lagged_messages,
            market_data_tick_drain_last: request.stats.market_data_tick_drain_last,
            market_data_tick_drain_max: request.stats.market_data_tick_drain_max,
            polymarket_ws_text_frames: request.stats.polymarket_ws_diagnostics.snapshot().text_frames,
            polymarket_ws_price_change_messages: request
                .stats
                .polymarket_ws_diagnostics
                .snapshot()
                .price_change_messages,
            polymarket_ws_book_messages: request.stats.polymarket_ws_diagnostics.snapshot().book_messages,
            polymarket_ws_orderbook_updates: request
                .stats
                .polymarket_ws_diagnostics
                .snapshot()
                .orderbook_updates,
            strategy_health: StrategyHealthView::default(),
        },
        connections: aggregate_connections_from_seed_cache(market_seeds, request.now_ms),
        recent_events: request.recent_events.clone(),
        recent_trades: request.recent_trades.clone(),
        recent_commands: request.recent_commands.clone(),
    };

    if request.apply_strategy_health {
        apply_strategy_health_to_monitor_state(
            &mut monitor_state,
            &request.warmup_status_by_symbol,
            request.min_warmup_samples,
            request.warmup_total_markets,
            request.warmup_completed_markets,
            request.warmup_failed_markets,
        );
    }

    let readiness_alert = if request.apply_strategy_health && request.emit_strategy_readiness_alert
    {
        strategy_readiness_alert(
            &monitor_state.runtime.strategy_health,
            request.now_ms.saturating_sub(request.start_time_ms),
            request.stats.signals_seen,
        )
    } else {
        None
    };

    MultiMonitorRenderResult {
        monitor_state,
        stats: request.stats.clone(),
        tick_index: request.tick_index,
        now_ms: request.now_ms,
        has_monitor_clients: request.has_monitor_clients,
        readiness_alert,
    }
}

fn build_single_monitor_state_from_seed_cache(
    settings: &Settings,
    market: &MarketConfig,
    engine: &SimpleAlphaEngine,
    observed: &ObservedState,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    let observed_map = HashMap::from([(market.symbol.clone(), observed.clone())]);
    let mut render_settings = settings.clone();
    render_settings.markets = vec![market.clone()];
    let request = build_monitor_render_request(
        &render_settings,
        stats,
        &observed_map,
        engine,
        risk,
        trade_book,
        command_center,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        recent_events,
        trading_mode,
        HashMap::new(),
        0,
        0,
        0,
        0,
        true,
        false,
        0,
        false,
    );
    let market_seeds = HashMap::from([(
        market.symbol.clone(),
        build_monitor_market_seed_from_observed(&market.symbol, observed, engine, execution, risk),
    )]);

    build_multi_monitor_state_from_seed_cache(&request, &market_seeds).monitor_state
}

fn build_multi_monitor_state_via_seed_cache(
    settings: &Settings,
    markets: &[MarketConfig],
    engine: &SimpleAlphaEngine,
    observed_map: &HashMap<Symbol, ObservedState>,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    let mut render_settings = settings.clone();
    render_settings.markets = markets.to_vec();
    let request = build_monitor_render_request(
        &render_settings,
        stats,
        observed_map,
        engine,
        risk,
        trade_book,
        command_center,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        recent_events,
        trading_mode,
        HashMap::new(),
        0,
        0,
        0,
        0,
        true,
        false,
        0,
        false,
    );
    let market_seeds = markets
        .iter()
        .map(|market| {
            (
                market.symbol.clone(),
                build_monitor_market_seed(
                    &market.symbol,
                    observed_map,
                    engine,
                    execution,
                    risk,
                ),
            )
        })
        .collect::<HashMap<_, _>>();

    build_multi_monitor_state_from_seed_cache(&request, &market_seeds).monitor_state
}

#[cfg(test)]
fn build_monitor_state_common(
    settings: &Settings,
    markets: &[MarketConfig],
    engine: &SimpleAlphaEngine,
    observed_map: &HashMap<Symbol, ObservedState>,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    let ws_diagnostics = stats.polymarket_ws_diagnostics.snapshot();
    let grouped_positions = group_positions_by_symbol(risk, now_ms);
    let position_views = build_position_views(
        settings,
        &grouped_positions,
        observed_map,
        engine,
        trade_book,
        now_ms,
    );
    let open_unrealized_pnl: f64 = position_views
        .iter()
        .map(|position| position.total_pnl_usd)
        .sum();
    let realized_pnl_usd: f64 = risk
        .build_snapshot(now_ms)
        .positions
        .values()
        .map(|position| position.realized_pnl.0.to_f64().unwrap_or_default())
        .sum();
    let total_pnl_usd = stats.total_pnl_usd + realized_pnl_usd + open_unrealized_pnl;
    let initial_capital = runtime_initial_capital(settings, trading_mode);
    let equity = initial_capital + total_pnl_usd;
    trade_book.push_equity(now_ms, equity);

    let connections = aggregate_connections(observed_map, now_ms);
    let market_views = build_market_views(
        settings,
        markets,
        observed_map,
        engine,
        execution,
        &position_views,
        now_ms,
    );
    let recent_commands = command_center
        .lock()
        .map(|center| center.recent_commands())
        .unwrap_or_default();

    MonitorState {
        schema_version: PLANNING_SCHEMA_VERSION,
        timestamp_ms: now_ms,
        mode: trading_mode,
        uptime_secs: (now_ms.saturating_sub(start_time_ms)) / 1000,
        paused,
        emergency,
        performance: PerformanceMetrics {
            total_pnl_usd,
            today_pnl_usd: trade_book.today_pnl_usd(equity, initial_capital, now_ms),
            win_rate_pct: trade_book.win_rate_pct(),
            max_drawdown_pct: trade_book.max_drawdown_pct(),
            profit_factor: trade_book.profit_factor(),
            equity,
            initial_capital,
        },
        positions: position_views,
        markets: market_views,
        signals: polyalpha_core::SignalStats {
            seen: stats.signals_seen as u64,
            rejected: stats.signals_rejected as u64,
            rejection_reasons: stats.rejection_reasons.clone(),
        },
        evaluation: EvaluationStats {
            attempts: stats.evaluation_attempts,
            evaluable_passes: stats.evaluable_passes,
            skipped: stats.evaluation_skipped,
            execution_rejected: stats.execution_rejected,
            skip_reasons: stats.skip_reasons.clone(),
            evaluable_status_counts: stats.evaluable_status_counts.clone(),
            async_classification_counts: stats.async_classification_counts.clone(),
        },
        config: build_monitor_strategy_config(settings, markets),
        runtime: MonitorRuntimeStats {
            snapshot_resync_count: stats.snapshot_resync_count as u64,
            funding_refresh_count: stats.funding_refresh_count as u64,
            market_data_rx_lag_events: stats.market_data_rx_lag_events,
            market_data_rx_lagged_messages: stats.market_data_rx_lagged_messages,
            market_data_tick_drain_last: stats.market_data_tick_drain_last,
            market_data_tick_drain_max: stats.market_data_tick_drain_max,
            polymarket_ws_text_frames: ws_diagnostics.text_frames,
            polymarket_ws_price_change_messages: ws_diagnostics.price_change_messages,
            polymarket_ws_book_messages: ws_diagnostics.book_messages,
            polymarket_ws_orderbook_updates: ws_diagnostics.orderbook_updates,
            strategy_health: StrategyHealthView::default(),
        },
        connections,
        recent_events: recent_events.to_vec(),
        recent_trades: trade_book.recent_trades(),
        recent_commands,
    }
}

#[cfg(test)]
fn build_multi_monitor_state_legacy(
    settings: &Settings,
    markets: &[MarketConfig],
    engine: &SimpleAlphaEngine,
    observed_map: &HashMap<Symbol, ObservedState>,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    build_monitor_state_common(
        settings,
        markets,
        engine,
        observed_map,
        stats,
        risk,
        execution,
        trade_book,
        command_center,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        recent_events,
        trading_mode,
    )
}

fn build_monitor_strategy_config(
    settings: &Settings,
    markets: &[MarketConfig],
) -> MonitorStrategyConfig {
    let basis = global_basis_config(settings);
    let per_asset = {
        let mut grouped = BTreeMap::new();
        for market in markets {
            let effective = effective_basis_config_for_market(settings, market);
            grouped
                .entry(effective.asset_key.clone())
                .or_insert_with(|| MonitorAssetStrategyConfig {
                    asset: effective.asset_key.to_ascii_uppercase(),
                    entry_z: effective
                        .entry_z_score_threshold
                        .to_f64()
                        .unwrap_or_default(),
                    exit_z: effective
                        .exit_z_score_threshold
                        .to_f64()
                        .unwrap_or_default(),
                    position_notional_usd: effective
                        .max_position_usd
                        .0
                        .to_f64()
                        .unwrap_or_default(),
                    rolling_window: effective.rolling_window_secs,
                    band_policy: effective.band_policy.to_string(),
                    min_poly_price: effective
                        .min_poly_price
                        .and_then(|value| value.to_f64())
                        .unwrap_or_default(),
                    max_poly_price: effective
                        .max_poly_price
                        .and_then(|value| value.to_f64())
                        .unwrap_or(1.0),
                    max_open_instant_loss_pct_of_budget: effective
                        .max_open_instant_loss_pct_of_budget
                        .to_f64()
                        .unwrap_or_default(),
                    delta_rebalance_threshold: effective
                        .delta_rebalance_threshold
                        .to_f64()
                        .unwrap_or_default(),
                    delta_rebalance_interval_secs: effective.delta_rebalance_interval_secs,
                    enable_freshness_check: effective.enable_freshness_check,
                    reject_on_disconnect: effective.reject_on_disconnect,
                });
        }
        grouped.into_values().collect::<Vec<_>>()
    };
    MonitorStrategyConfig {
        entry_z: basis.entry_z_score_threshold.to_f64().unwrap_or_default(),
        exit_z: basis.exit_z_score_threshold.to_f64().unwrap_or_default(),
        position_notional_usd: basis.max_position_usd.0.to_f64().unwrap_or_default(),
        rolling_window: basis.rolling_window_secs,
        band_policy: basis.band_policy.to_string(),
        market_data_mode: match settings.strategy.market_data.mode {
            MarketDataMode::Poll => "poll".to_owned(),
            MarketDataMode::Ws => "ws".to_owned(),
        },
        max_stale_ms: settings.strategy.market_data.max_stale_ms,
        poly_open_max_quote_age_ms: settings
            .strategy
            .market_data
            .resolved_poly_open_max_quote_age_ms(),
        cex_open_max_quote_age_ms: settings
            .strategy
            .market_data
            .resolved_cex_open_max_quote_age_ms(),
        close_max_quote_age_ms: settings
            .strategy
            .market_data
            .resolved_close_max_quote_age_ms(),
        max_cross_leg_skew_ms: settings
            .strategy
            .market_data
            .resolved_max_cross_leg_skew_ms(),
        borderline_poly_quote_age_ms: settings
            .strategy
            .market_data
            .resolved_borderline_poly_quote_age_ms(),
        min_poly_price: basis
            .min_poly_price
            .and_then(|value| value.to_f64())
            .unwrap_or_default(),
        max_poly_price: basis
            .max_poly_price
            .and_then(|value| value.to_f64())
            .unwrap_or(1.0),
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
        allow_partial_fill: settings.paper_slippage.allow_partial_fill,
        enable_freshness_check: basis.enable_freshness_check,
        reject_on_disconnect: basis.reject_on_disconnect,
        per_asset,
    }
}

fn build_monitor_events(recent_events: &VecDeque<PaperEventLog>) -> Vec<MonitorEvent> {
    let mut selected = Vec::new();
    let mut selected_seq = HashSet::new();
    let mut pinned_trade_closed_count = 0usize;
    let mut low_priority_count = 0usize;

    for event in recent_events.iter().rev() {
        if event.kind != "trade_closed" {
            continue;
        }
        if pinned_trade_closed_count >= MAX_MONITOR_PINNED_TRADE_CLOSED_EVENTS {
            break;
        }
        selected.push(event);
        selected_seq.insert(event.seq);
        pinned_trade_closed_count += 1;
    }

    for event in recent_events.iter().rev() {
        if selected_seq.contains(&event.seq) {
            continue;
        }
        if is_low_priority_recent_event_kind(&event.kind) {
            if low_priority_count >= MAX_MONITOR_LOW_PRIORITY_EVENTS {
                continue;
            }
            low_priority_count += 1;
        }
        selected.push(event);
        if selected.len() == MAX_MONITOR_EVENTS {
            break;
        }
    }

    selected.sort_by_key(|event| std::cmp::Reverse(event.seq));

    selected
        .into_iter()
        .map(paper_event_to_monitor_event)
        .collect()
}

fn paper_event_to_monitor_event(event: &PaperEventLog) -> MonitorEvent {
    MonitorEvent {
        schema_version: PLANNING_SCHEMA_VERSION,
        timestamp_ms: event.timestamp_ms,
        kind: classify_monitor_event_kind(&event.kind),
        market: event.market.clone(),
        correlation_id: monitor_event_correlation_id(event),
        summary: event.summary.clone(),
        details: event.details.clone(),
        payload: event.payload.clone(),
    }
}

fn broadcast_monitor_events_since(
    recent_events: &VecDeque<PaperEventLog>,
    last_event_seq_sent: &mut u64,
    event_broadcaster: &tokio::sync::broadcast::Sender<MonitorEvent>,
) {
    let last_sent = *last_event_seq_sent;
    for event in recent_events.iter().filter(|event| event.seq > last_sent) {
        let _ = event_broadcaster.send(paper_event_to_monitor_event(event));
        *last_event_seq_sent = event.seq;
    }
}

fn classify_monitor_event_kind(kind: &str) -> EventKind {
    match kind {
        "signal" => EventKind::Signal,
        "gate" => EventKind::Gate,
        "plan" | "recovery" | "result" | "fill" | "execution" => EventKind::Trade,
        "skip" => EventKind::Skip,
        "risk" | "price_filter" | "spread_filter" | "paused" | "emergency" | "stale"
        | "reconcile" => EventKind::Risk,
        "error" | "poll_error" => EventKind::Error,
        "trade_closed" => EventKind::TradeClosed,
        "warning" | "state" | "market_closed" | "command" | "connection" | "market_phase"
        | "market_data" => EventKind::System,
        _ => EventKind::System,
    }
}

fn monitor_event_correlation_id(event: &PaperEventLog) -> Option<String> {
    event.correlation_id.clone()
}

#[cfg(test)]
fn build_market_views(
    settings: &Settings,
    markets: &[MarketConfig],
    observed_map: &HashMap<Symbol, ObservedState>,
    engine: &SimpleAlphaEngine,
    execution: &ExecutionManager<RuntimeExecutor>,
    position_views: &[polyalpha_core::PositionView],
    now_ms: u64,
) -> Vec<MarketView> {
    markets
        .iter()
        .map(|market| {
            let basis = effective_basis_config_for_market(settings, market);
            let thresholds = freshness_thresholds(settings, &basis);
            let require_connections = require_connected_inputs(&basis);
            let observed = observed_map.get(&market.symbol);
            let snapshot = engine.basis_snapshot(&market.symbol);
            let (yes_basis_history_len, no_basis_history_len) =
                engine.basis_history_lengths(&market.symbol);
            let active_token_side_value = snapshot.as_ref().map(|item| item.token_side);
            let active_token_side = snapshot
                .as_ref()
                .map(|item| format!("{:?}", item.token_side).to_ascii_uppercase());
            let has_position = position_views
                .iter()
                .any(|position| position.market == market.symbol.0);
            let poly_yes_price =
                observed.and_then(|item| item.poly_yes_mid.and_then(|value| value.to_f64()));
            let poly_no_price =
                observed.and_then(|item| item.poly_no_mid.and_then(|value| value.to_f64()));
            let poly_price = match snapshot.as_ref().map(|item| item.token_side) {
                Some(TokenSide::Yes) => poly_yes_price.or(poly_no_price),
                Some(TokenSide::No) => poly_no_price.or(poly_yes_price),
                None => poly_yes_price.or(poly_no_price),
            };
            let async_snapshot = observed.map(|item| {
                classify_async_market(
                    now_ms,
                    market,
                    item,
                    thresholds,
                    active_token_side_value,
                    false,
                    require_connections,
                )
            });
            let retention_reason = market_retention_reason(
                has_position,
                execution.active_plan_priority(&market.symbol),
            );
            let near_opportunity = market_is_near_opportunity(settings, snapshot.as_ref());
            let semantics_guard_active =
                market_open_semantics_guard_reason(market, &settings.strategy.market_scope, now_ms)
                    .is_some();
            let market_tier = determine_market_tier(
                settings,
                !semantics_guard_active
                    && async_snapshot
                        .as_ref()
                        .is_some_and(async_snapshot_is_tradeable),
                retention_reason,
                !semantics_guard_active && near_opportunity,
                observed.and_then(|item| item.market_pool.last_focus_at_ms),
                observed.and_then(|item| item.market_pool.last_tradeable_at_ms),
                observed
                    .and_then(|item| market_pool_cooldown_reason(item, now_ms))
                    .is_some(),
                now_ms,
            );
            MarketView {
                symbol: market.symbol.0.clone(),
                z_score: snapshot.as_ref().and_then(|item| item.z_score),
                basis_pct: snapshot.as_ref().map(|item| item.signal_basis * 100.0),
                poly_price,
                poly_yes_price,
                poly_no_price,
                cex_price: observed.and_then(|item| item.cex_mid.and_then(|value| value.to_f64())),
                fair_value: snapshot.as_ref().map(|item| item.fair_value),
                has_position,
                minutes_to_expiry: snapshot.as_ref().map(|item| item.minutes_to_expiry),
                basis_history_len: yes_basis_history_len.max(no_basis_history_len),
                raw_sigma: snapshot.as_ref().and_then(|item| item.raw_sigma),
                effective_sigma: snapshot.as_ref().and_then(|item| item.sigma),
                sigma_source: snapshot.as_ref().map(|item| item.sigma_source.clone()),
                returns_window_len: snapshot
                    .as_ref()
                    .map(|item| item.returns_window_len)
                    .unwrap_or_default(),
                active_token_side,
                poly_updated_at_ms: async_snapshot
                    .as_ref()
                    .and_then(|item| item.poly_updated_at_ms),
                cex_updated_at_ms: async_snapshot
                    .as_ref()
                    .and_then(|item| item.cex_updated_at_ms),
                poly_quote_age_ms: async_snapshot
                    .as_ref()
                    .and_then(|item| item.poly_quote_age_ms),
                cex_quote_age_ms: async_snapshot
                    .as_ref()
                    .and_then(|item| item.cex_quote_age_ms),
                cross_leg_skew_ms: async_snapshot
                    .as_ref()
                    .and_then(|item| item.cross_leg_skew_ms),
                evaluable_status: async_snapshot
                    .as_ref()
                    .map(|item| item.evaluable_status)
                    .unwrap_or_default(),
                async_classification: async_snapshot
                    .as_ref()
                    .map(|item| item.async_classification)
                    .unwrap_or_default(),
                market_tier,
                retention_reason,
                last_focus_at_ms: observed.and_then(|item| item.market_pool.last_focus_at_ms),
                last_tradeable_at_ms: observed
                    .and_then(|item| item.market_pool.last_tradeable_at_ms),
            }
        })
        .collect()
}

fn build_position_views(
    settings: &Settings,
    grouped_positions: &HashMap<Symbol, GroupedPosition>,
    observed_map: &HashMap<Symbol, ObservedState>,
    engine: &SimpleAlphaEngine,
    trade_book: &TradeBook,
    now_ms: u64,
) -> Vec<polyalpha_core::PositionView> {
    let mut out = Vec::new();
    for (symbol, grouped) in grouped_positions {
        let observed = observed_map.get(symbol);
        let signal_snapshot = engine.basis_snapshot(symbol);
        let open_trade = trade_book.open.get(symbol);

        let (poly_position, token_side) = if let Some(position) = grouped.poly_yes.as_ref() {
            (Some(position), Some(TokenSide::Yes))
        } else if let Some(position) = grouped.poly_no.as_ref() {
            (Some(position), Some(TokenSide::No))
        } else {
            (None, None)
        };

        let poly_entry_price = open_trade
            .and_then(|item| item.poly_entry_price)
            .or_else(|| {
                poly_position.map(|position| position.entry_price.0.to_f64().unwrap_or_default())
            })
            .unwrap_or_default();
        let poly_current_price = match token_side {
            Some(TokenSide::Yes) => observed
                .and_then(|item| item.poly_yes_mid.and_then(|value| value.to_f64()))
                .unwrap_or_default(),
            Some(TokenSide::No) => observed
                .and_then(|item| item.poly_no_mid.and_then(|value| value.to_f64()))
                .unwrap_or_default(),
            None => 0.0,
        };
        let poly_quantity = poly_position
            .map(|position| quantity_to_f64(position.quantity))
            .unwrap_or_default();
        let poly_side = match (poly_position, token_side) {
            (Some(position), Some(token_side)) => {
                format!("{:?} {:?}", position.side, token_side).to_ascii_uppercase()
            }
            _ => "FLAT".to_owned(),
        };
        let poly_pnl_usd = poly_position
            .map(|position| {
                signed_pnl_for_position(
                    position.side,
                    poly_entry_price,
                    poly_current_price,
                    poly_quantity,
                )
            })
            .unwrap_or_default();

        let cex_position = grouped.cex.as_ref();
        let cex_entry_price = open_trade
            .and_then(|item| item.cex_entry_price)
            .or_else(|| {
                cex_position.map(|position| position.entry_price.0.to_f64().unwrap_or_default())
            })
            .unwrap_or_default();
        let cex_current_price = observed
            .and_then(|item| item.cex_mid.and_then(|value| value.to_f64()))
            .unwrap_or_default();
        let cex_quantity = cex_position
            .map(|position| quantity_to_f64(position.quantity))
            .unwrap_or_default();
        let cex_side = cex_position
            .map(|position| format!("{:?}", position.side).to_ascii_uppercase())
            .unwrap_or_else(|| "FLAT".to_owned());
        let cex_exchange = cex_position
            .map(|position| format!("{:?}", position.key.exchange))
            .unwrap_or_else(|| "Binance".to_owned());
        let cex_pnl_usd = cex_position
            .map(|position| {
                signed_pnl_for_position(
                    position.side,
                    cex_entry_price,
                    cex_current_price,
                    cex_quantity,
                )
            })
            .unwrap_or_default();

        out.push(polyalpha_core::PositionView {
            market: symbol.0.clone(),
            poly_side,
            poly_entry_price,
            poly_current_price,
            poly_quantity,
            poly_pnl_usd,
            poly_spread_pct: 0.0,
            poly_slippage_bps: settings.paper_slippage.poly_slippage_bps as f64,
            cex_exchange,
            cex_side,
            cex_entry_price,
            cex_current_price,
            cex_quantity,
            cex_pnl_usd,
            cex_spread_pct: 0.0,
            cex_slippage_bps: settings.paper_slippage.cex_slippage_bps as f64,
            basis_entry_bps: open_trade
                .map(|item| item.basis_entry_bps)
                .unwrap_or_default(),
            basis_current_bps: signal_snapshot
                .as_ref()
                .map(|item| (item.signal_basis * 10_000.0).round() as i32)
                .unwrap_or_default(),
            delta: open_trade
                .map(|item| item.delta)
                .or_else(|| signal_snapshot.as_ref().map(|item| item.delta))
                .unwrap_or_default(),
            hedge_ratio: open_trade.map(|item| item.hedge_ratio).unwrap_or(1.0),
            total_pnl_usd: poly_pnl_usd + cex_pnl_usd,
            holding_secs: open_trade
                .filter(|item| item.opened_at_ms > 0)
                .map(|item| now_ms.saturating_sub(item.opened_at_ms) / 1000)
                .unwrap_or_default(),
        });
    }
    out.sort_by(|left, right| {
        right
            .total_pnl_usd
            .partial_cmp(&left.total_pnl_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

fn aggregate_connections(
    observed_map: &HashMap<Symbol, ObservedState>,
    now_ms: u64,
) -> HashMap<String, ConnectionInfo> {
    let mut out = HashMap::new();
    for observed in observed_map.values() {
        for (name, status) in &observed.connections {
            let updated_at_ms = observed.connection_updated_at_ms.get(name).copied();
            let reconnect_count = observed
                .connection_reconnect_count
                .get(name)
                .copied()
                .unwrap_or_default();
            let disconnect_count = observed
                .connection_disconnect_count
                .get(name)
                .copied()
                .unwrap_or_default();
            let last_disconnect_at_ms =
                observed.connection_last_disconnect_at_ms.get(name).copied();
            out.entry(name.clone())
                .and_modify(|info: &mut ConnectionInfo| {
                    info.status = *status;
                    if updated_at_ms.unwrap_or_default() > info.updated_at_ms.unwrap_or_default() {
                        info.updated_at_ms = updated_at_ms;
                    }
                    info.reconnect_count = info.reconnect_count.max(reconnect_count);
                    info.disconnect_count = info.disconnect_count.max(disconnect_count);
                    if last_disconnect_at_ms.unwrap_or_default()
                        > info.last_disconnect_at_ms.unwrap_or_default()
                    {
                        info.last_disconnect_at_ms = last_disconnect_at_ms;
                    }
                })
                .or_insert(ConnectionInfo {
                    status: *status,
                    latency_ms: None,
                    updated_at_ms,
                    transport_idle_ms: updated_at_ms.map(|ts| now_ms.saturating_sub(ts)),
                    reconnect_count,
                    disconnect_count,
                    last_disconnect_at_ms,
                });
        }
    }
    for info in out.values_mut() {
        info.transport_idle_ms = info.updated_at_ms.map(|ts| now_ms.saturating_sub(ts));
    }
    out
}

fn quantity_to_f64(quantity: VenueQuantity) -> f64 {
    match quantity {
        VenueQuantity::PolyShares(shares) => shares.0.to_f64().unwrap_or_default(),
        VenueQuantity::CexBaseQty(qty) => qty.0.to_f64().unwrap_or_default(),
    }
}

fn signed_pnl_for_position(
    side: polyalpha_core::PositionSide,
    entry_price: f64,
    current_price: f64,
    quantity: f64,
) -> f64 {
    if quantity <= f64::EPSILON || current_price <= 0.0 {
        return 0.0;
    }
    match side {
        polyalpha_core::PositionSide::Long => (current_price - entry_price) * quantity,
        polyalpha_core::PositionSide::Short => (entry_price - current_price) * quantity,
        polyalpha_core::PositionSide::Flat => 0.0,
    }
}

fn register_open_trade_from_candidate(
    trade_book: &mut TradeBook,
    candidate: &OpenCandidate,
    engine: &SimpleAlphaEngine,
) {
    let Some(snapshot) = engine.basis_snapshot(&candidate.symbol) else {
        return;
    };
    let basis_entry_bps = (snapshot.signal_basis * 10_000.0).round() as i32;
    trade_book.register_open_signal(candidate, basis_entry_bps, 1.0, candidate.delta_estimate);
}

async fn build_paper_execution_stack(
    settings: &Settings,
    registry: &SymbolRegistry,
    executor_start_ms: u64,
    execution_mode: RuntimeExecutionMode,
    live_market: Option<&MarketConfig>,
) -> Result<(
    Arc<InMemoryOrderbookProvider>,
    RuntimeExecutor,
    ExecutionManager<RuntimeExecutor>,
)> {
    build_execution_stack(
        settings,
        registry,
        execution_mode,
        executor_start_ms,
        live_market,
    )
    .await
}

fn update_paper_orderbook(
    provider: &InMemoryOrderbookProvider,
    registry: &SymbolRegistry,
    snapshot: &polyalpha_core::OrderBookSnapshot,
) {
    apply_orderbook_snapshot(provider, registry, snapshot);
}

fn market_symbols_for_event(event: &MarketDataEvent, registry: &SymbolRegistry) -> Vec<Symbol> {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => vec![snapshot.symbol.clone()],
        MarketDataEvent::TradeUpdate { symbol, .. }
        | MarketDataEvent::FundingRate { symbol, .. }
        | MarketDataEvent::MarketLifecycle { symbol, .. } => vec![symbol.clone()],
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange,
            venue_symbol,
            ..
        }
        | MarketDataEvent::CexVenueTradeUpdate {
            exchange,
            venue_symbol,
            ..
        }
        | MarketDataEvent::CexVenueFundingRate {
            exchange,
            venue_symbol,
            ..
        } => registry
            .lookup_cex_symbols(*exchange, venue_symbol)
            .map(|symbols| symbols.to_vec())
            .unwrap_or_default(),
        MarketDataEvent::ConnectionEvent { .. } => Vec::new(),
    }
}

fn update_paper_orderbook_for_event(
    provider: &InMemoryOrderbookProvider,
    registry: &SymbolRegistry,
    event: &MarketDataEvent,
) {
    match event {
        MarketDataEvent::OrderBookUpdate { snapshot } => {
            update_paper_orderbook(provider, registry, snapshot);
        }
        MarketDataEvent::CexVenueOrderBookUpdate {
            exchange,
            venue_symbol,
            bids,
            asks,
            exchange_timestamp_ms,
            received_at_ms,
            sequence,
        } => {
            let Some(symbols) = registry.lookup_cex_symbols(*exchange, venue_symbol) else {
                return;
            };
            let Some(first_symbol) = symbols.first() else {
                return;
            };
            provider.update_cex_shared(
                polyalpha_core::OrderBookSnapshot {
                    exchange: *exchange,
                    symbol: first_symbol.clone(),
                    instrument: InstrumentKind::CexPerp,
                    bids: bids.clone(),
                    asks: asks.clone(),
                    exchange_timestamp_ms: *exchange_timestamp_ms,
                    received_at_ms: *received_at_ms,
                    sequence: *sequence,
                    last_trade_price: None,
                },
                venue_symbol.clone(),
                symbols,
            );
        }
        _ => {}
    }
}

fn record_signal_rejection(stats: &mut PaperStats, reason: impl Into<String>) {
    let reason = reason.into();
    *stats.rejection_reasons.entry(reason).or_insert(0) += 1;
}

fn is_evaluation_attempt_event(event: &MarketDataEvent) -> bool {
    matches!(
        event,
        MarketDataEvent::OrderBookUpdate { snapshot }
            if matches!(snapshot.instrument, InstrumentKind::CexPerp)
    ) || matches!(
        event,
        MarketDataEvent::TradeUpdate { instrument, .. }
            if matches!(instrument, InstrumentKind::CexPerp)
    ) || matches!(
        event,
        MarketDataEvent::CexVenueOrderBookUpdate { .. } | MarketDataEvent::CexVenueTradeUpdate { .. }
    )
}

fn warning_symbol(warning: &EngineWarning) -> &Symbol {
    match warning {
        EngineWarning::ConnectionLost { symbol, .. }
        | EngineWarning::NoCexData { symbol }
        | EngineWarning::NoPolyData { symbol }
        | EngineWarning::CexPriceStale { symbol, .. }
        | EngineWarning::PolyPriceStale { symbol, .. }
        | EngineWarning::DataMisaligned { symbol, .. } => symbol,
    }
}

fn evaluation_skip_reason_from_warning(warning: &EngineWarning) -> EvaluationSkipReason {
    match warning {
        EngineWarning::ConnectionLost { .. } => EvaluationSkipReason::ConnectionLost,
        EngineWarning::NoCexData { .. } => EvaluationSkipReason::NoCexReference,
        EngineWarning::NoPolyData { .. } => EvaluationSkipReason::NoFreshPolySample,
        EngineWarning::CexPriceStale { .. } => EvaluationSkipReason::CexPriceStale,
        EngineWarning::PolyPriceStale { .. } => EvaluationSkipReason::PolyPriceStale,
        EngineWarning::DataMisaligned { .. } => EvaluationSkipReason::DataMisaligned,
    }
}

fn primary_evaluation_skip_warning<'a>(
    warnings: &'a [EngineWarning],
) -> Option<(EvaluationSkipReason, &'a EngineWarning)> {
    let mut seen = HashSet::new();
    warnings.iter().find_map(|warning| {
        let reason = evaluation_skip_reason_from_warning(warning);
        if seen.insert(reason) {
            Some((reason, warning))
        } else {
            None
        }
    })
}

fn increment_counter(map: &mut HashMap<String, u64>, key: &str) {
    *map.entry(key.to_owned()).or_insert(0) += 1;
}

fn record_evaluation_attempt(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    event: &MarketDataEvent,
    warnings: &[EngineWarning],
    observed: Option<&AuditObservedMarket>,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    if !is_evaluation_attempt_event(event) {
        return Ok(());
    }

    stats.evaluation_attempts += 1;
    if let Some(observed) = observed {
        increment_counter(
            &mut stats.evaluable_status_counts,
            observed.evaluable_status.as_str(),
        );
        increment_counter(
            &mut stats.async_classification_counts,
            observed.async_classification.as_str(),
        );
    }

    let Some((reason, warning)) = primary_evaluation_skip_warning(warnings) else {
        stats.evaluable_passes += 1;
        return Ok(());
    };

    stats.evaluation_skipped += 1;
    increment_counter(&mut stats.skip_reasons, reason.key());

    let symbol = warning_symbol(warning);
    let now_ms = now_millis();
    let dedupe_key = format!("{}:{}", symbol.0, reason.key());
    let should_emit = stats
        .last_skip_event_ms
        .get(&dedupe_key)
        .map(|last_seen_ms| now_ms.saturating_sub(*last_seen_ms) >= SKIP_EVENT_DEBOUNCE_MS)
        .unwrap_or(true);

    if should_emit {
        stats.last_skip_event_ms.insert(dedupe_key, now_ms);
        push_event_with_context(
            recent_events,
            "skip",
            summarize_engine_warning(warning),
            Some(symbol.0.clone()),
            None,
            None,
            build_skip_event_details(reason, warning, observed),
        );
        if let Some(audit) = audit.as_mut() {
            audit.record_evaluation_skip(event, warnings, observed.cloned())?;
        }
    }
    Ok(())
}

fn record_evaluation_attempts_for_symbols(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    event: &MarketDataEvent,
    symbols: &[Symbol],
    warnings: &[EngineWarning],
    observed_by_symbol: &HashMap<Symbol, AuditObservedMarket>,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    if !is_evaluation_attempt_event(event) {
        return Ok(());
    }

    if symbols.is_empty() {
        return record_evaluation_attempt(stats, recent_events, event, warnings, None, audit);
    }

    let mut warnings_by_symbol: HashMap<Symbol, Vec<EngineWarning>> = HashMap::new();
    for warning in warnings {
        warnings_by_symbol
            .entry(warning_symbol(warning).clone())
            .or_default()
            .push(warning.clone());
    }

    for symbol in symbols {
        let symbol_warnings = warnings_by_symbol
            .get(symbol)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        record_evaluation_attempt(
            stats,
            recent_events,
            event,
            symbol_warnings,
            observed_by_symbol.get(symbol),
            audit,
        )?;
    }

    Ok(())
}

fn reject_signal(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    candidate: &OpenCandidate,
    kind: &str,
    reason: impl Into<String>,
    summary: String,
    details: Option<EventDetails>,
) {
    stats.signals_rejected += 1;
    record_signal_rejection(stats, reason);
    push_signal_event(recent_events, kind, summary, candidate, details);
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

fn track_open_signal_execution_outcome(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trade_book: &mut TradeBook,
    candidate: &OpenCandidate,
    events: &[ExecutionEvent],
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    let any_fill = events
        .iter()
        .any(|event| matches!(event, ExecutionEvent::OrderFilled(_)));
    let rejection_reasons = opening_signal_rejection_reasons(events);
    if any_fill || rejection_reasons.is_empty() {
        return Ok(());
    }

    stats.signals_rejected += 1;
    stats.execution_rejected += 1;
    for reason in &rejection_reasons {
        record_signal_rejection(stats, reason.clone());
    }
    trade_book.discard_open_trade(&candidate.symbol);
    push_signal_event(
        recent_events,
        "risk",
        format!(
            "候选提交后被拒绝 {}（{}）",
            candidate.candidate_id,
            rejection_reasons.join(", ")
        ),
        candidate,
        build_post_submit_rejection_details(candidate, &rejection_reasons),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_open_signal_rejected_post_submit(candidate, &rejection_reasons)?;
    }
    Ok(())
}

fn track_intent_execution_rejection(
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    trigger: &RuntimeTrigger,
    message: &str,
    audit: &mut Option<PaperAudit>,
) -> Result<bool> {
    let reason_code = match extract_plan_rejection_reason(message) {
        Some(reason) => reason,
        None => return Ok(false),
    };

    stats.execution_rejected += 1;
    record_signal_rejection(stats, reason_code.clone());
    push_event_with_context_and_payload(
        recent_events,
        "risk",
        format!(
            "{} {}未执行（{}）",
            trigger.symbol().0,
            trigger_action_label_zh(trigger),
            translate_trade_plan_rejection(&reason_code)
        ),
        Some(trigger.symbol().0.clone()),
        Some(trigger.id().to_owned()),
        Some(trigger.correlation_id().to_owned()),
        build_intent_rejection_details(trigger, &reason_code, message),
        Some(intent_rejection_payload(trigger, message)),
    );
    if let Some(audit) = audit.as_mut() {
        audit.record_intent_rejected(trigger, &reason_code, message)?;
    }
    Ok(true)
}

fn finalize_audit_on_runtime_exit(
    audit: &mut Option<PaperAudit>,
    stats: &PaperStats,
    monitor_state: &MonitorState,
    tick_index: usize,
    now_ms: u64,
) -> Result<()> {
    if let Some(audit) = audit.as_mut() {
        audit.finalize(stats, monitor_state, tick_index, now_ms)?;
    }
    Ok(())
}

fn finish_runtime_with_audit(
    runtime_result: Result<()>,
    audit: &mut Option<PaperAudit>,
    stats: &PaperStats,
    monitor_state: &MonitorState,
    tick_index: usize,
    now_ms: u64,
) -> Result<()> {
    let finalize_result =
        finalize_audit_on_runtime_exit(audit, stats, monitor_state, tick_index, now_ms);
    match (runtime_result, finalize_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(runtime_err), Ok(())) => Err(runtime_err),
        (Ok(()), Err(finalize_err)) => Err(finalize_err),
        (Err(runtime_err), Err(finalize_err)) => {
            tracing::error!("审计收口失败（主错误保留）：{finalize_err}");
            Err(runtime_err.context(format!("审计收口也失败：{finalize_err}")))
        }
    }
}

fn local_day_key(timestamp_ms: u64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms as i64)
        .single()
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_owned())
}

fn build_multi_snapshot(
    markets: &[MarketConfig],
    tick_index: usize,
    engine: &SimpleAlphaEngine,
    observed_map: &HashMap<Symbol, ObservedState>,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    executor: &RuntimeExecutor,
) -> Result<PaperSnapshot> {
    let risk_snapshot = risk.build_snapshot(now_millis());
    let focus_market = markets
        .iter()
        .max_by(|left, right| {
            let rank = |market: &MarketConfig| {
                let has_position = risk_snapshot.positions.values().any(|position| {
                    position.key.symbol == market.symbol
                        && quantity_to_f64(position.quantity) > f64::EPSILON
                });
                let observed = observed_map.get(&market.symbol);
                let has_observed = observed
                    .map(|state| {
                        state.poly_yes_mid.is_some()
                            || state.poly_no_mid.is_some()
                            || state.cex_mid.is_some()
                            || state.market_phase.is_some()
                    })
                    .unwrap_or(false);
                let signal_strength = engine
                    .basis_snapshot(&market.symbol)
                    .map(|snapshot| {
                        snapshot
                            .z_score
                            .map(f64::abs)
                            .unwrap_or_else(|| snapshot.signal_basis.abs())
                    })
                    .unwrap_or(0.0);
                (has_position, signal_strength, has_observed)
            };

            let left_rank = rank(left);
            let right_rank = rank(right);
            left_rank
                .0
                .cmp(&right_rank.0)
                .then_with(|| left_rank.1.total_cmp(&right_rank.1))
                .then_with(|| left_rank.2.cmp(&right_rank.2))
        })
        .ok_or_else(|| anyhow!("没有可用市场"))?;
    let observed = observed_map
        .get(&focus_market.symbol)
        .cloned()
        .unwrap_or_default();
    let aggregated_connections = observed_map
        .values()
        .flat_map(|state| state.connections.iter())
        .fold(HashMap::new(), |mut acc, (exchange, status)| {
            acc.insert(exchange.clone(), *status);
            acc
        });

    let positions = risk_snapshot
        .positions
        .values()
        .map(position_view_from_position)
        .collect::<Vec<_>>();
    let signal_snapshot = engine.basis_snapshot(&focus_market.symbol);

    Ok(PaperSnapshot {
        tick_index,
        market: format!(
            "{} 个市场（焦点：{}）",
            markets.len(),
            focus_market.symbol.0
        ),
        hedge_exchange: format!("{:?}", focus_market.hedge_exchange),
        hedge_symbol: focus_market.cex_symbol.clone(),
        market_phase: observed
            .market_phase
            .clone()
            .map(|phase| format!("{phase:?}"))
            .unwrap_or_else(|| "Trading".to_owned()),
        connections: sorted_connections(&aggregated_connections),
        poly_yes_mid: observed.poly_yes_mid.and_then(|value| value.to_f64()),
        poly_no_mid: observed.poly_no_mid.and_then(|value| value.to_f64()),
        cex_mid: observed.cex_mid.and_then(|value| value.to_f64()),
        current_basis: signal_snapshot.as_ref().map(|s| s.signal_basis),
        signal_token_side: signal_snapshot
            .as_ref()
            .map(|s| format!("{:?}", s.token_side)),
        current_zscore: signal_snapshot.as_ref().and_then(|s| s.z_score),
        current_fair_value: signal_snapshot.as_ref().map(|s| s.fair_value),
        current_delta: signal_snapshot.as_ref().map(|s| s.delta),
        cex_reference_price: signal_snapshot.as_ref().map(|s| s.cex_reference_price),
        hedge_state: execution
            .hedge_state(&focus_market.symbol)
            .map(|state| format!("{state:?}"))
            .unwrap_or_else(|| "Idle".to_owned()),
        last_signal_id: execution
            .last_signal_id(&focus_market.symbol)
            .map(|value| value.to_owned()),
        open_orders: executor.open_order_count()?,
        active_dmm_orders: execution.active_dmm_order_count(&focus_market.symbol),
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

fn build_multi_monitor_state(
    settings: &Settings,
    markets: &[MarketConfig],
    engine: &SimpleAlphaEngine,
    observed_map: &HashMap<Symbol, ObservedState>,
    stats: &PaperStats,
    risk: &InMemoryRiskManager,
    execution: &ExecutionManager<RuntimeExecutor>,
    trade_book: &mut TradeBook,
    command_center: &Arc<Mutex<CommandCenter>>,
    paused: bool,
    emergency: bool,
    start_time_ms: u64,
    now_ms: u64,
    recent_events: &[MonitorEvent],
    trading_mode: polyalpha_core::TradingMode,
) -> MonitorState {
    build_multi_monitor_state_via_seed_cache(
        settings,
        markets,
        engine,
        observed_map,
        stats,
        risk,
        execution,
        trade_book,
        command_center,
        paused,
        emergency,
        start_time_ms,
        now_ms,
        recent_events,
        trading_mode,
    )
}

/// Extract asset prefix from CEX symbol (e.g., "BTCUSDT" -> "btc")
fn extract_asset_prefix(cex_symbol: &str) -> String {
    asset_key_from_cex_symbol(cex_symbol)
}

/// Build per-market parameter overrides from config
fn build_market_overrides(
    settings: &Settings,
    markets: &[MarketConfig],
) -> HashMap<Symbol, MarketOverrideConfig> {
    let mut overrides = HashMap::new();

    for market in markets {
        let asset_key = extract_asset_prefix(&market.cex_symbol);
        if settings.strategy.basis.overrides.contains_key(&asset_key) {
            let basis = settings.strategy.basis.effective_for_asset_key(&asset_key);
            let market_override = MarketOverrideConfig {
                entry_z: basis.entry_z_score_threshold.to_f64(),
                exit_z: basis.exit_z_score_threshold.to_f64(),
                rolling_window_minutes: Some((basis.rolling_window_secs / 60) as usize),
                min_warmup_samples: Some(basis.min_warmup_samples),
                min_basis_bps: basis.min_basis_bps.to_f64(),
                position_notional_usd: Some(basis.max_position_usd),
                enable_freshness_check: Some(basis.enable_freshness_check),
                reject_on_disconnect: Some(basis.reject_on_disconnect),
                max_poly_data_age_ms: Some(basis.max_data_age_minutes.saturating_mul(60_000)),
                max_cex_data_age_ms: Some(basis.max_data_age_minutes.saturating_mul(60_000)),
                max_time_diff_ms: Some(basis.max_time_diff_minutes.saturating_mul(60_000)),
            };
            overrides.insert(market.symbol.clone(), market_override);
        }
    }

    overrides
}

fn format_ws_close_reason(
    frame: Option<&tokio_tungstenite::tungstenite::protocol::CloseFrame>,
) -> String {
    match frame {
        Some(frame) if frame.reason.is_empty() => format!("code={}", frame.code),
        Some(frame) => format!("code={} reason={}", frame.code, frame.reason),
        None => "peer closed without frame".to_owned(),
    }
}

fn log_ws_warning(message: impl AsRef<str>) {
    let message = message.as_ref();
    eprintln!("{message}");
    tracing::warn!("{message}");
}

async fn run_ws_future_with_timeout<F, T>(
    future: F,
    timeout_duration: Duration,
    label: String,
) -> Result<T>
where
    F: Future<Output = std::result::Result<T, tokio_tungstenite::tungstenite::Error>>,
{
    match tokio::time::timeout(timeout_duration, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(anyhow!("{label} failed: {err}")),
        Err(_) => Err(anyhow!(
            "{label} timed out after {}ms",
            timeout_duration.as_millis()
        )),
    }
}

async fn abort_join_handles(handles: &mut Vec<JoinHandle<()>>) {
    for handle in handles.iter() {
        handle.abort();
    }
    while let Some(handle) = handles.pop() {
        let _ = handle.await;
    }
}

async fn abort_optional_join_handle(handle: &mut Option<JoinHandle<()>>) {
    if let Some(handle) = handle.take() {
        handle.abort();
        let _ = handle.await;
    }
}

fn ws_connect_delay_ms(base_backoff_ms: u64, stream_key: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    stream_key.hash(&mut hasher);
    let jitter_ms = hasher.finish() % 2_000;
    base_backoff_ms.saturating_add(jitter_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_core::{
        AuditConfig, BasisStrategyConfig, BinanceConfig, CexBaseQty, DmmStrategyConfig,
        GeneralConfig, MarketDataConfig, NegRiskStrategyConfig, OkxConfig, PaperSlippageConfig,
        PaperTradingConfig, PolymarketConfig, RiskConfig, SettlementRules, StrategyConfig,
        PolymarketIds,
    };
    use polyalpha_executor::OrderbookProvider;
    use serde_json::json;

    fn cex_evaluation_event() -> MarketDataEvent {
        MarketDataEvent::OrderBookUpdate {
            snapshot: polyalpha_core::OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-test"),
                instrument: InstrumentKind::CexPerp,
                bids: Vec::new(),
                asks: Vec::new(),
                exchange_timestamp_ms: 1,
                received_at_ms: 1,
                sequence: 1,
                last_trade_price: None,
            },
        }
    }

    fn open_candidate(token_side: TokenSide) -> OpenCandidate {
        OpenCandidate {
            schema_version: PLANNING_SCHEMA_VERSION,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-test"),
            token_side,
            direction: "long".to_owned(),
            fair_value: 0.47,
            raw_mispricing: 0.05,
            delta_estimate: 0.25,
            risk_budget_usd: 100.0,
            strength: SignalStrength::Normal,
            z_score: Some(2.1),
            raw_sigma: None,
            effective_sigma: None,
            sigma_source: None,
            returns_window_len: 0,
            timestamp_ms: 1,
        }
    }

    fn sample_planning_diagnostics() -> PlanningDiagnostics {
        PlanningDiagnostics {
            planner_depth_levels: 5,
            poly_mid_price: Some(Price(Decimal::new(27, 2))),
            poly_best_bid: Some(Price(Decimal::new(26, 2))),
            poly_best_ask: Some(Price(Decimal::new(28, 2))),
            poly_book_average_price: Some(Price(Decimal::new(275, 3))),
            poly_executable_price: Some(Price(Decimal::new(27864, 5))),
            poly_price_impact_bps: Some(Decimal::new(320, 0)),
            poly_requested_shares: PolyShares(Decimal::new(720, 0)),
            poly_planned_shares: PolyShares(Decimal::new(720, 0)),
            cex_planned_qty: polyalpha_core::CexBaseQty(Decimal::new(864, 5)),
            planned_edge_usd: Some(UsdNotional(Decimal::new(1134, 2))),
            instant_open_loss_usd: Some(UsdNotional(Decimal::new(796, 2))),
            poly_bids_top: vec![
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(26, 2)),
                    shares: PolyShares(Decimal::new(100, 0)),
                },
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(259, 3)),
                    shares: PolyShares(Decimal::new(80, 0)),
                },
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(258, 3)),
                    shares: PolyShares(Decimal::new(60, 0)),
                },
            ],
            poly_asks_top: vec![
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(28, 2)),
                    shares: PolyShares(Decimal::new(120, 0)),
                },
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(281, 3)),
                    shares: PolyShares(Decimal::new(90, 0)),
                },
                polyalpha_core::PlanningBookLevel {
                    price: Price(Decimal::new(282, 3)),
                    shares: PolyShares(Decimal::new(70, 0)),
                },
            ],
        }
    }

    fn rejected_order_submitted_event(reason: &str) -> ExecutionEvent {
        ExecutionEvent::OrderSubmitted {
            symbol: Symbol::new("btc-test"),
            exchange: Exchange::Binance,
            response: polyalpha_core::OrderResponse {
                client_order_id: polyalpha_core::ClientOrderId("client-1".to_owned()),
                exchange_order_id: polyalpha_core::OrderId("ord-1".to_owned()),
                status: OrderStatus::Rejected,
                filled_quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(
                    Decimal::ZERO,
                )),
                average_price: None,
                rejection_reason: Some(reason.to_owned()),
                timestamp_ms: 1,
            },
            correlation_id: "corr-1".to_owned(),
        }
    }

    fn poly_fill(token_side: TokenSide) -> Fill {
        Fill {
            fill_id: "fill-poly-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-test"),
            instrument: match token_side {
                TokenSide::Yes => InstrumentKind::PolyYes,
                TokenSide::No => InstrumentKind::PolyNo,
            },
            order_id: polyalpha_core::OrderId("ord-poly-1".to_owned()),
            side: polyalpha_core::OrderSide::Buy,
            price: Price(Decimal::new(45, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10, 0))),
            notional_usd: UsdNotional(Decimal::new(45, 0)),
            fee: UsdNotional::ZERO,
            is_maker: false,
            timestamp_ms: 1,
        }
    }

    fn cex_fill(side: polyalpha_core::OrderSide) -> Fill {
        Fill {
            fill_id: "fill-cex-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-test"),
            instrument: InstrumentKind::CexPerp,
            order_id: polyalpha_core::OrderId("ord-cex-1".to_owned()),
            side,
            price: Price(Decimal::new(100_000, 0)),
            quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(1, 3))),
            notional_usd: UsdNotional(Decimal::new(100, 0)),
            fee: UsdNotional::ZERO,
            is_maker: false,
            timestamp_ms: 1,
        }
    }

    fn cex_fill_with_price(
        side: polyalpha_core::OrderSide,
        price: i64,
        fee: i64,
        timestamp_ms: u64,
    ) -> Fill {
        Fill {
            fill_id: format!("fill-cex-{timestamp_ms}"),
            correlation_id: format!("corr-{timestamp_ms}"),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-test"),
            instrument: InstrumentKind::CexPerp,
            order_id: polyalpha_core::OrderId(format!("ord-cex-{timestamp_ms}")),
            side,
            price: Price(Decimal::new(price, 0)),
            quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(1, 3))),
            notional_usd: UsdNotional(Decimal::new(price / 1000, 0)),
            fee: UsdNotional(Decimal::new(fee, 0)),
            is_maker: false,
            timestamp_ms,
        }
    }

    fn sample_execution_result_with_funding(funding_cost_usd: i64) -> ExecutionResult {
        ExecutionResult {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: "plan-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-test"),
            status: "completed".to_owned(),
            poly_order_ledger: Vec::new(),
            cex_order_ledger: Vec::new(),
            actual_poly_cost_usd: UsdNotional::ZERO,
            actual_cex_cost_usd: UsdNotional(Decimal::new(100, 0)),
            actual_poly_fee_usd: UsdNotional::ZERO,
            actual_cex_fee_usd: UsdNotional::ZERO,
            actual_funding_cost_usd: UsdNotional(Decimal::new(funding_cost_usd, 0)),
            realized_edge_usd: UsdNotional::ZERO,
            plan_vs_fill_deviation_usd: UsdNotional::ZERO,
            actual_residual_delta: 0.0,
            actual_shock_loss_up_1pct: UsdNotional::ZERO,
            actual_shock_loss_down_1pct: UsdNotional::ZERO,
            actual_shock_loss_up_2pct: UsdNotional::ZERO,
            actual_shock_loss_down_2pct: UsdNotional::ZERO,
            recovery_required: false,
            timestamp_ms: 1,
        }
    }

    fn sample_trade_plan() -> TradePlan {
        TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: "plan-1".to_owned(),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: "idem-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-test"),
            intent_type: "open_position".to_owned(),
            priority: "open_position".to_owned(),
            recovery_decision_reason: None,
            created_at_ms: 1_000,
            poly_exchange_timestamp_ms: 1_000,
            poly_received_at_ms: 1_001,
            poly_sequence: 11,
            cex_exchange_timestamp_ms: 1_002,
            cex_received_at_ms: 1_003,
            cex_sequence: 21,
            plan_hash: "hash-1".to_owned(),
            poly_side: polyalpha_core::OrderSide::Buy,
            poly_token_side: TokenSide::Yes,
            poly_sizing_mode: "buy_budget_cap".to_owned(),
            poly_requested_shares: PolyShares(Decimal::new(10, 0)),
            poly_planned_shares: PolyShares(Decimal::new(10, 0)),
            poly_max_cost_usd: UsdNotional(Decimal::new(100, 0)),
            poly_max_avg_price: Price(Decimal::new(35, 2)),
            poly_max_shares: PolyShares(Decimal::new(10, 0)),
            poly_min_avg_price: Price(Decimal::ZERO),
            poly_min_proceeds_usd: UsdNotional::ZERO,
            poly_book_avg_price: Price(Decimal::new(34, 2)),
            poly_executable_price: Price(Decimal::new(35, 2)),
            poly_friction_cost_usd: UsdNotional(Decimal::new(2, 1)),
            poly_fee_usd: UsdNotional(Decimal::new(1, 1)),
            cex_side: polyalpha_core::OrderSide::Sell,
            cex_planned_qty: polyalpha_core::CexBaseQty(Decimal::new(25, 3)),
            cex_book_avg_price: Price(Decimal::new(100_000, 0)),
            cex_executable_price: Price(Decimal::new(100_010, 0)),
            cex_friction_cost_usd: UsdNotional(Decimal::new(2, 1)),
            cex_fee_usd: UsdNotional(Decimal::new(1, 1)),
            raw_edge_usd: UsdNotional(Decimal::new(12, 0)),
            planned_edge_usd: UsdNotional(Decimal::new(10, 0)),
            expected_funding_cost_usd: UsdNotional(Decimal::new(1, 1)),
            residual_risk_penalty_usd: UsdNotional(Decimal::new(1, 1)),
            post_rounding_residual_delta: 0.01,
            shock_loss_up_1pct: UsdNotional(Decimal::new(3, 0)),
            shock_loss_down_1pct: UsdNotional(Decimal::new(3, 0)),
            shock_loss_up_2pct: UsdNotional(Decimal::new(6, 0)),
            shock_loss_down_2pct: UsdNotional(Decimal::new(6, 0)),
            plan_ttl_ms: 250,
            max_poly_sequence_drift: 1,
            max_cex_sequence_drift: 1,
            max_poly_price_move: Price(Decimal::new(1, 3)),
            max_cex_price_move: Price(Decimal::new(50, 0)),
            min_planned_edge_usd: UsdNotional(Decimal::new(1, 0)),
            max_residual_delta: 0.05,
            max_shock_loss_usd: UsdNotional(Decimal::new(8, 0)),
            max_plan_vs_fill_deviation_usd: UsdNotional(Decimal::new(5, 0)),
        }
    }

    fn poly_book_snapshot(
        token_side: TokenSide,
        sequence: u64,
    ) -> polyalpha_core::OrderBookSnapshot {
        polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-test"),
            instrument: match token_side {
                TokenSide::Yes => InstrumentKind::PolyYes,
                TokenSide::No => InstrumentKind::PolyNo,
            },
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(44, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(46, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
            }],
            exchange_timestamp_ms: 1_000 + sequence,
            received_at_ms: 1_100 + sequence,
            sequence,
            last_trade_price: None,
        }
    }

    fn cex_book_snapshot(sequence: u64) -> polyalpha_core::OrderBookSnapshot {
        polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-test"),
            instrument: InstrumentKind::CexPerp,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(99_990, 0)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                    500, 0,
                ))),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(100_010, 0)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                    500, 0,
                ))),
            }],
            exchange_timestamp_ms: 2_000 + sequence,
            received_at_ms: 2_100 + sequence,
            sequence,
            last_trade_price: None,
        }
    }

    fn seed_runtime_books(
        provider: &InMemoryOrderbookProvider,
        registry: &SymbolRegistry,
        sequence: u64,
    ) {
        update_paper_orderbook(
            provider,
            registry,
            &poly_book_snapshot(TokenSide::Yes, sequence),
        );
        update_paper_orderbook(
            provider,
            registry,
            &poly_book_snapshot(TokenSide::No, sequence),
        );
        update_paper_orderbook(provider, registry, &cex_book_snapshot(sequence));
    }

    fn test_recent_event(kind: &str, summary: impl Into<String>) -> PaperEventLog {
        PaperEventLog {
            seq: 0,
            timestamp_ms: 1,
            kind: kind.to_owned(),
            summary: summary.into(),
            market: None,
            signal_id: None,
            correlation_id: None,
            details: None,
            payload: None,
        }
    }

    fn test_market(symbol: &str, exchange: Exchange, cex_symbol: &str) -> MarketConfig {
        let strike = match cex_symbol {
            "BTCUSDT" => Decimal::new(70_000, 0),
            "ETHUSDT" => Decimal::new(2_000, 0),
            "SOLUSDT" => Decimal::new(100, 0),
            "XRPUSDT" => Decimal::new(1, 0),
            _ => Decimal::ONE,
        };
        let question = match cex_symbol {
            "BTCUSDT" => "Will the price of Bitcoin be above $70,000 on April 7?",
            "ETHUSDT" => "Will the price of Ethereum be above $2,000 on April 7?",
            "SOLUSDT" => "Will the price of Solana be above $100 on April 7?",
            "XRPUSDT" => "Will the price of XRP be above $1 on April 7?",
            _ => "Will the price of this asset be above $1 on April 7?",
        };
        MarketConfig {
            symbol: Symbol::new(symbol),
            poly_ids: polyalpha_core::PolymarketIds {
                condition_id: format!("{symbol}-cond"),
                yes_token_id: format!("{symbol}-yes"),
                no_token_id: format!("{symbol}-no"),
            },
            market_question: Some(question.to_owned()),
            market_rule: Some(polyalpha_core::MarketRule {
                kind: polyalpha_core::MarketRuleKind::Above,
                lower_strike: Some(Price(strike)),
                upper_strike: None,
            }),
            cex_symbol: cex_symbol.to_owned(),
            hedge_exchange: exchange,
            strike_price: Some(Price(strike)),
            settlement_timestamp: 0,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn test_settings_with_price_filter(
        min_price: Option<Decimal>,
        max_price: Option<Decimal>,
    ) -> Settings {
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
            markets: vec![MarketConfig {
                symbol: Symbol::new("btc-test"),
                poly_ids: polyalpha_core::PolymarketIds {
                    condition_id: "cond".to_owned(),
                    yes_token_id: "yes".to_owned(),
                    no_token_id: "no".to_owned(),
                },
                market_question: Some(
                    "Will the price of Bitcoin be above $70,000 on April 7?".to_owned(),
                ),
                market_rule: Some(polyalpha_core::MarketRule {
                    kind: polyalpha_core::MarketRuleKind::Above,
                    lower_strike: Some(Price(Decimal::new(70_000, 0))),
                    upper_strike: None,
                }),
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: Some(Price(Decimal::new(70_000, 0))),
                settlement_timestamp: 0,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            }],
            strategy: StrategyConfig {
                basis: BasisStrategyConfig {
                    entry_z_score_threshold: Decimal::new(2, 0),
                    exit_z_score_threshold: Decimal::new(5, 1),
                    rolling_window_secs: 3_600,
                    min_warmup_samples: 100,
                    min_basis_bps: Decimal::new(50, 0),
                    max_position_usd: UsdNotional(Decimal::new(100, 0)),
                    max_open_instant_loss_pct_of_budget: Decimal::new(4, 2),
                    delta_rebalance_threshold: Decimal::new(5, 2),
                    delta_rebalance_interval_secs: 60,
                    band_policy: polyalpha_core::BandPolicyMode::ConfiguredBand,
                    min_poly_price: min_price,
                    max_poly_price: max_price,
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
                market_scope: StrategyMarketScopeConfig::default(),
                market_data: MarketDataConfig::default(),
            },
            risk: RiskConfig {
                max_total_exposure_usd: UsdNotional(Decimal::new(10_000, 0)),
                max_single_position_usd: UsdNotional(Decimal::new(1_000, 0)),
                max_daily_loss_usd: UsdNotional(Decimal::new(500, 0)),
                max_drawdown_pct: Decimal::new(5, 0),
                max_open_orders: 10,
                circuit_breaker_cooldown_secs: 60,
                rate_limit_orders_per_sec: 5,
                max_persistence_lag_secs: 10,
            },
            paper: PaperTradingConfig::default(),
            paper_slippage: PaperSlippageConfig::default(),
            execution_costs: polyalpha_core::ExecutionCostConfig::default(),
            audit: AuditConfig::default(),
        }
    }

    fn test_settings_with_per_asset_basis_overrides() -> Settings {
        let mut settings =
            test_settings_with_price_filter(Some(Decimal::new(20, 2)), Some(Decimal::new(50, 2)));
        settings.markets = vec![
            test_market("btc-test", Exchange::Binance, "BTCUSDT"),
            test_market("eth-test", Exchange::Binance, "ETHUSDT"),
        ];
        settings.strategy.basis.overrides.insert(
            "btc".to_owned(),
            polyalpha_core::BasisOverrideConfig {
                min_poly_price: Some(Decimal::new(10, 2)),
                max_poly_price: Some(Decimal::new(45, 2)),
                max_open_instant_loss_pct_of_budget: Some(Decimal::new(2, 2)),
                delta_rebalance_threshold: Some(Decimal::new(8, 2)),
                delta_rebalance_interval_secs: Some(120),
                max_data_age_minutes: Some(2),
                max_time_diff_minutes: Some(3),
                enable_freshness_check: Some(false),
                reject_on_disconnect: Some(false),
                ..Default::default()
            },
        );
        settings
    }

    fn test_settings_with_audit_data_dir(data_dir: String) -> Settings {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.general.data_dir = data_dir;
        settings.audit.enabled = true;
        settings.audit.warehouse_sync_interval_secs = 1;
        settings
    }

    fn unique_test_data_dir(label: &str) -> String {
        std::env::temp_dir()
            .join(format!("polyalpha-{label}-{}", Uuid::new_v4()))
            .display()
            .to_string()
    }

    fn break_audit_warehouse_path(data_dir: &str) {
        let warehouse_dir = PathBuf::from(data_dir).join("audit").join("warehouse");
        let _ = fs::remove_dir_all(&warehouse_dir);
        fs::write(&warehouse_dir, b"blocked").expect("replace warehouse dir with file");
    }

    #[test]
    fn diagnostics_polymarket_ws_snapshot_accumulates_counts() {
        let diagnostics = PolymarketWsDiagnostics::default();

        diagnostics.record_text_frame();
        diagnostics.record_text_frame();
        diagnostics.record_price_change_message();
        diagnostics.record_book_message();
        diagnostics.add_orderbook_updates(3);

        assert_eq!(
            diagnostics.snapshot(),
            PolymarketWsDiagnosticsSnapshot {
                text_frames: 2,
                price_change_messages: 1,
                book_messages: 1,
                orderbook_updates: 3,
            }
        );
    }

    #[test]
    fn diagnostics_market_data_lag_and_drain_helpers_accumulate() {
        let mut stats = PaperStats::default();
        let mut processed_since_last_tick = 2_u64;

        record_market_data_rx_lag(&mut stats, 11);
        record_market_data_rx_lag(&mut stats, 7);
        record_market_data_tick_drain(&mut stats, &mut processed_since_last_tick);

        assert_eq!(stats.market_data_rx_lag_events, 2);
        assert_eq!(stats.market_data_rx_lagged_messages, 18);
        assert_eq!(stats.market_data_tick_drain_last, 2);
        assert_eq!(stats.market_data_tick_drain_max, 2);
        assert_eq!(processed_since_last_tick, 0);

        record_market_data_tick_drain(&mut stats, &mut processed_since_last_tick);
        assert_eq!(stats.market_data_tick_drain_last, 0);
        assert_eq!(stats.market_data_tick_drain_max, 2);
    }

    #[test]
    fn diagnostics_audit_counter_snapshot_includes_runtime_counters() {
        let mut stats = PaperStats::default();
        stats.market_data_rx_lag_events = 4;
        stats.market_data_rx_lagged_messages = 29;
        stats.market_data_tick_drain_last = 9;
        stats.market_data_tick_drain_max = 15;
        stats.polymarket_ws_diagnostics.record_text_frame();
        stats.polymarket_ws_diagnostics.record_text_frame();
        stats
            .polymarket_ws_diagnostics
            .record_price_change_message();
        stats.polymarket_ws_diagnostics.record_book_message();
        stats.polymarket_ws_diagnostics.add_orderbook_updates(5);

        let snapshot = audit_counter_snapshot(&stats);

        assert_eq!(snapshot.market_data_rx_lag_events, 4);
        assert_eq!(snapshot.market_data_rx_lagged_messages, 29);
        assert_eq!(snapshot.market_data_tick_drain_last, 9);
        assert_eq!(snapshot.market_data_tick_drain_max, 15);
        assert_eq!(snapshot.polymarket_ws_text_frames, 2);
        assert_eq!(snapshot.polymarket_ws_price_change_messages, 1);
        assert_eq!(snapshot.polymarket_ws_book_messages, 1);
        assert_eq!(snapshot.polymarket_ws_orderbook_updates, 5);
    }

    #[test]
    fn diagnostics_build_multi_monitor_state_surfaces_runtime_counters() {
        let settings = test_settings_with_price_filter(None, None);
        let markets = settings.markets.clone();
        let engine =
            SimpleAlphaEngine::with_markets(build_paper_engine_config(&settings), markets.clone());
        let observed = HashMap::from([(markets[0].symbol.clone(), ObservedState::default())]);
        let mut stats = PaperStats::default();
        stats.market_data_rx_lag_events = 3;
        stats.market_data_rx_lagged_messages = 21;
        stats.market_data_tick_drain_last = 6;
        stats.market_data_tick_drain_max = 12;
        stats.polymarket_ws_diagnostics.record_text_frame();
        stats
            .polymarket_ws_diagnostics
            .record_price_change_message();
        stats.polymarket_ws_diagnostics.record_book_message();
        stats.polymarket_ws_diagnostics.add_orderbook_updates(4);

        let risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let execution = ExecutionManager::new(RuntimeExecutor::DryRun(DryRunExecutor::default()));
        let command_center = Arc::new(Mutex::new(CommandCenter::default()));
        let mut trade_book = TradeBook::default();

        let monitor = build_multi_monitor_state(
            &settings,
            &markets,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut trade_book,
            &command_center,
            false,
            false,
            0,
            1_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        assert_eq!(monitor.runtime.market_data_rx_lag_events, 3);
        assert_eq!(monitor.runtime.market_data_rx_lagged_messages, 21);
        assert_eq!(monitor.runtime.market_data_tick_drain_last, 6);
        assert_eq!(monitor.runtime.market_data_tick_drain_max, 12);
        assert_eq!(monitor.runtime.polymarket_ws_text_frames, 1);
        assert_eq!(monitor.runtime.polymarket_ws_price_change_messages, 1);
        assert_eq!(monitor.runtime.polymarket_ws_book_messages, 1);
        assert_eq!(monitor.runtime.polymarket_ws_orderbook_updates, 4);
    }

    #[test]
    fn build_multi_monitor_state_via_seed_cache_matches_public_wrapper() {
        let settings = test_settings_with_price_filter(None, None);
        let markets = settings.markets.clone();
        let engine =
            SimpleAlphaEngine::with_markets(build_paper_engine_config(&settings), markets.clone());
        let observed = HashMap::from([(
            markets[0].symbol.clone(),
            ObservedState {
                poly_yes_mid: Some(Decimal::new(48, 2)),
                poly_no_mid: Some(Decimal::new(52, 2)),
                cex_mid: Some(Decimal::new(81_000, 0)),
                poly_yes_updated_at_ms: Some(1_900),
                cex_updated_at_ms: Some(1_950),
                connections: HashMap::from([
                    ("Polymarket".to_owned(), ConnectionStatus::Connected),
                    ("Binance".to_owned(), ConnectionStatus::Connected),
                ]),
                connection_updated_at_ms: HashMap::from([
                    ("Polymarket".to_owned(), 1_900),
                    ("Binance".to_owned(), 1_950),
                ]),
                ..ObservedState::default()
            },
        )]);
        let mut stats = PaperStats::default();
        stats.market_data_rx_lag_events = 3;
        stats.market_data_rx_lagged_messages = 21;
        stats.market_data_tick_drain_last = 6;
        stats.market_data_tick_drain_max = 12;
        stats.polymarket_ws_diagnostics.record_text_frame();
        stats
            .polymarket_ws_diagnostics
            .record_price_change_message();
        stats.polymarket_ws_diagnostics.record_book_message();
        stats.polymarket_ws_diagnostics.add_orderbook_updates(4);

        let risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let execution = ExecutionManager::new(RuntimeExecutor::DryRun(DryRunExecutor::default()));
        let command_center = Arc::new(Mutex::new(CommandCenter::default()));

        let mut legacy_trade_book = TradeBook::default();
        let legacy = build_multi_monitor_state_legacy(
            &settings,
            &markets,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut legacy_trade_book,
            &command_center,
            false,
            false,
            1_000,
            2_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        let mut reused_trade_book = TradeBook::default();
        let reused = build_multi_monitor_state_via_seed_cache(
            &settings,
            &markets,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut reused_trade_book,
            &command_center,
            false,
            false,
            1_000,
            2_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        assert_eq!(reused.markets.len(), legacy.markets.len());
        assert_eq!(reused.markets[0].symbol, legacy.markets[0].symbol);
        assert_eq!(reused.markets[0].poly_yes_price, legacy.markets[0].poly_yes_price);
        assert_eq!(reused.markets[0].poly_no_price, legacy.markets[0].poly_no_price);
        assert_eq!(reused.markets[0].cex_price, legacy.markets[0].cex_price);
        assert_eq!(
            reused
                .connections
                .get("Binance")
                .and_then(|info| info.updated_at_ms),
            legacy
                .connections
                .get("Binance")
                .and_then(|info| info.updated_at_ms)
        );
        assert_eq!(
            reused.runtime.market_data_rx_lag_events,
            legacy.runtime.market_data_rx_lag_events
        );
        assert_eq!(
            reused.runtime.polymarket_ws_orderbook_updates,
            legacy.runtime.polymarket_ws_orderbook_updates
        );
        assert_eq!(
            reused.performance.total_pnl_usd,
            legacy.performance.total_pnl_usd
        );
    }

    #[test]
    fn per_asset_basis_price_filter_uses_market_override() {
        let settings = test_settings_with_per_asset_basis_overrides();
        let mut observed_map = HashMap::new();
        observed_map.insert(
            Symbol::new("btc-test"),
            ObservedState {
                poly_no_mid: Some(Decimal::new(15, 2)),
                ..ObservedState::default()
            },
        );
        observed_map.insert(
            Symbol::new("eth-test"),
            ObservedState {
                poly_no_mid: Some(Decimal::new(15, 2)),
                ..ObservedState::default()
            },
        );

        let btc_candidate = OpenCandidate {
            symbol: Symbol::new("btc-test"),
            token_side: TokenSide::No,
            ..open_candidate(TokenSide::No)
        };
        let eth_candidate = OpenCandidate {
            symbol: Symbol::new("eth-test"),
            token_side: TokenSide::No,
            ..open_candidate(TokenSide::No)
        };

        assert!(candidate_passes_price_filter_multi(
            &btc_candidate,
            &observed_map,
            &settings
        ));
        assert!(!candidate_passes_price_filter_multi(
            &eth_candidate,
            &observed_map,
            &settings
        ));
    }

    #[test]
    fn per_asset_basis_builds_engine_overrides_and_monitor_summary() {
        let settings = test_settings_with_per_asset_basis_overrides();
        let overrides = build_market_overrides(&settings, &settings.markets);
        let btc_symbol = Symbol::new("btc-test");

        let btc_override = overrides
            .get(&btc_symbol)
            .expect("btc override should exist");
        assert_eq!(btc_override.max_poly_data_age_ms, Some(120_000));
        assert_eq!(btc_override.max_time_diff_ms, Some(180_000));
        assert_eq!(btc_override.enable_freshness_check, Some(false));
        assert_eq!(btc_override.reject_on_disconnect, Some(false));

        let monitor = build_monitor_strategy_config(&settings, &settings.markets);
        assert_eq!(monitor.per_asset.len(), 2);

        let btc = monitor
            .per_asset
            .iter()
            .find(|item| item.asset == "BTC")
            .expect("btc monitor config");
        assert_eq!(btc.min_poly_price, 0.10);
        assert_eq!(btc.max_poly_price, 0.45);
        assert!(!btc.enable_freshness_check);

        let eth = monitor
            .per_asset
            .iter()
            .find(|item| item.asset == "ETH")
            .expect("eth monitor config");
        assert_eq!(eth.min_poly_price, 0.20);
        assert_eq!(eth.max_poly_price, 0.50);
        assert!(eth.enable_freshness_check);
    }

    #[test]
    fn collect_unique_funding_targets_dedupes_by_exchange_and_symbol() {
        let markets = vec![
            test_market("eth-reach-2k-mar", Exchange::Binance, "ETHUSDT"),
            test_market("eth-reach-2k-apr", Exchange::Binance, "ETHUSDT"),
            test_market("btc-dip-80k-mar", Exchange::Binance, "BTCUSDT"),
            test_market("eth-reach-2k-okx", Exchange::Okx, "ETH-USDT-SWAP"),
            test_market("eth-reach-2k-okx-2", Exchange::Okx, "ETH-USDT-SWAP"),
        ];

        let targets = collect_unique_funding_targets(&markets);

        assert_eq!(
            targets,
            vec![
                FundingRefreshTarget {
                    exchange: Exchange::Binance,
                    venue_symbol: "BTCUSDT".to_owned(),
                },
                FundingRefreshTarget {
                    exchange: Exchange::Binance,
                    venue_symbol: "ETHUSDT".to_owned(),
                },
                FundingRefreshTarget {
                    exchange: Exchange::Okx,
                    venue_symbol: "ETH-USDT-SWAP".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn paper_artifact_rejects_incompatible_schema_version() {
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode: polyalpha_core::TradingMode::Paper,
            env: "test".to_owned(),
            market_index: 0,
            market: "btc-test".to_owned(),
            hedge_exchange: "Binance".to_owned(),
            market_data_mode: Some("poll".to_owned()),
            poll_interval_ms: 1_000,
            depth: 5,
            include_funding: false,
            ticks_processed: 0,
            final_snapshot: PaperSnapshot {
                tick_index: 0,
                market: "btc-test".to_owned(),
                hedge_exchange: "Binance".to_owned(),
                hedge_symbol: "BTCUSDT".to_owned(),
                market_phase: "Trading".to_owned(),
                connections: Vec::new(),
                poly_yes_mid: None,
                poly_no_mid: None,
                cex_mid: None,
                current_basis: None,
                signal_token_side: None,
                current_zscore: None,
                current_fair_value: None,
                current_delta: None,
                cex_reference_price: None,
                hedge_state: "Idle".to_owned(),
                last_signal_id: None,
                open_orders: 0,
                active_dmm_orders: 0,
                signals_seen: 0,
                signals_rejected: 0,
                order_submitted: 0,
                order_cancelled: 0,
                fills: 0,
                state_changes: 0,
                poll_errors: 0,
                total_exposure_usd: "0".to_owned(),
                daily_pnl_usd: "0".to_owned(),
                breaker: "Closed".to_owned(),
                positions: Vec::new(),
            },
            open_orders: Vec::new(),
            recent_events: Vec::new(),
            snapshots: Vec::new(),
        };
        let mut raw = serde_json::to_value(artifact).expect("serialize artifact");
        raw["schema_version"] = json!(PAPER_ARTIFACT_SCHEMA_VERSION + 1);

        let err = serde_json::from_value::<PaperArtifact>(raw).expect_err("schema mismatch");
        assert!(err
            .to_string()
            .contains("incompatible paper artifact schema_version"));
    }

    #[test]
    fn paper_artifact_defaults_missing_trading_mode_to_paper() {
        let artifact = PaperArtifact {
            schema_version: PAPER_ARTIFACT_SCHEMA_VERSION,
            trading_mode: polyalpha_core::TradingMode::Paper,
            env: "test".to_owned(),
            market_index: 0,
            market: "btc-test".to_owned(),
            hedge_exchange: "Binance".to_owned(),
            market_data_mode: Some("poll".to_owned()),
            poll_interval_ms: 1_000,
            depth: 5,
            include_funding: false,
            ticks_processed: 0,
            final_snapshot: PaperSnapshot {
                tick_index: 0,
                market: "btc-test".to_owned(),
                hedge_exchange: "Binance".to_owned(),
                hedge_symbol: "BTCUSDT".to_owned(),
                market_phase: "Trading".to_owned(),
                connections: Vec::new(),
                poly_yes_mid: None,
                poly_no_mid: None,
                cex_mid: None,
                current_basis: None,
                signal_token_side: None,
                current_zscore: None,
                current_fair_value: None,
                current_delta: None,
                cex_reference_price: None,
                hedge_state: "Idle".to_owned(),
                last_signal_id: None,
                open_orders: 0,
                active_dmm_orders: 0,
                signals_seen: 0,
                signals_rejected: 0,
                order_submitted: 0,
                order_cancelled: 0,
                fills: 0,
                state_changes: 0,
                poll_errors: 0,
                total_exposure_usd: "0".to_owned(),
                daily_pnl_usd: "0".to_owned(),
                breaker: "Closed".to_owned(),
                positions: Vec::new(),
            },
            open_orders: Vec::new(),
            recent_events: Vec::new(),
            snapshots: Vec::new(),
        };
        let mut raw = serde_json::to_value(artifact).expect("serialize artifact");
        raw.as_object_mut()
            .expect("artifact object")
            .remove("trading_mode");

        let artifact: PaperArtifact = serde_json::from_value(raw).expect("artifact without mode");

        assert_eq!(artifact.trading_mode, polyalpha_core::TradingMode::Paper);
    }

    #[test]
    fn artifact_path_uses_live_directory_for_live_mode() {
        let data_dir = unique_test_data_dir("live-artifact-path");
        let mut settings = test_settings_with_price_filter(None, None);
        settings.general.data_dir = data_dir.clone();

        let path = artifact_path(&settings, "paper-trial", polyalpha_core::TradingMode::Live)
            .expect("live artifact path");

        assert!(path.ends_with("live/paper-trial-last-run.json"));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn multi_ws_runtime_persists_live_artifact_even_when_ws_bootstrap_fails() {
        let data_dir = unique_test_data_dir("live-multi-ws-artifact");
        let socket_path = PathBuf::from(&data_dir).join("monitor.sock");
        let mut settings = test_settings_with_price_filter(None, None);
        settings.general.data_dir = data_dir.clone();
        settings.general.monitor_socket_path = socket_path.display().to_string();
        settings.strategy.market_data.mode = MarketDataMode::Ws;
        settings.polymarket.clob_api_url = "http://127.0.0.1:9".to_owned();
        settings.polymarket.ws_url = "ws://127.0.0.1:9/ws/market".to_owned();
        settings.binance.rest_url = "http://127.0.0.1:9".to_owned();
        settings.binance.ws_url = "ws://127.0.0.1:9/ws".to_owned();
        settings.okx.rest_url = "http://127.0.0.1:9".to_owned();
        settings.okx.ws_public_url = "ws://127.0.0.1:9/ws/public".to_owned();
        settings.okx.ws_private_url = "ws://127.0.0.1:9/ws/private".to_owned();
        settings.audit.enabled = false;

        run_paper_multi_ws_mode(
            "live-multi-artifact",
            settings.clone(),
            0,
            1,
            1,
            5,
            false,
            false,
            0,
            RuntimeExecutionMode::LiveMock,
            polyalpha_core::TradingMode::Live,
        )
        .await
        .expect("runtime should complete even when bootstrap endpoints fail");

        let artifact_path = artifact_path(
            &settings,
            "live-multi-artifact",
            polyalpha_core::TradingMode::Live,
        )
        .expect("artifact path");
        assert!(
            artifact_path.exists(),
            "multi-market ws runtime should persist live artifact at {}",
            artifact_path.display()
        );

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn market_retention_reason_prefers_active_plan_over_position() {
        assert_eq!(
            market_retention_reason(true, Some("force_exit")),
            polyalpha_core::MarketRetentionReason::ForceExit
        );
        assert_eq!(
            market_retention_reason(true, Some("residual_recovery")),
            polyalpha_core::MarketRetentionReason::ResidualRecovery
        );
        assert_eq!(
            market_retention_reason(true, Some("close_position")),
            polyalpha_core::MarketRetentionReason::CloseInProgress
        );
        assert_eq!(
            market_retention_reason(true, Some("delta_rebalance")),
            polyalpha_core::MarketRetentionReason::DeltaRebalance
        );
        assert_eq!(
            market_retention_reason(true, None),
            polyalpha_core::MarketRetentionReason::HasPosition
        );
        assert_eq!(
            market_retention_reason(false, None),
            polyalpha_core::MarketRetentionReason::None
        );
    }

    #[test]
    fn record_market_tier_activity_updates_focus_and_tradeable_timestamps() {
        let mut observed = ObservedState::default();
        note_focus_market_activity(&mut observed, 1_000);
        assert_eq!(observed.market_pool.last_focus_at_ms, Some(1_000));
        assert_eq!(observed.market_pool.last_tradeable_at_ms, None);

        note_tradeable_market_activity(&mut observed, 1_500);
        assert_eq!(observed.market_pool.last_focus_at_ms, Some(1_500));
        assert_eq!(observed.market_pool.last_tradeable_at_ms, Some(1_500));
        assert_eq!(observed.market_pool.open_cooldown_reason, None);
        assert_eq!(observed.market_pool.open_cooldown_until_ms, None);
    }

    #[test]
    fn determine_market_tier_distinguishes_tradeable_focus_and_observation() {
        let settings = test_settings_with_price_filter(None, None);
        assert_eq!(
            determine_market_tier(
                &settings,
                true,
                polyalpha_core::MarketRetentionReason::None,
                false,
                Some(1_990),
                Some(1_995),
                false,
                2_000,
            ),
            polyalpha_core::MarketTier::Tradeable
        );
        assert_eq!(
            determine_market_tier(
                &settings,
                true,
                polyalpha_core::MarketRetentionReason::None,
                true,
                Some(1_990),
                None,
                false,
                2_000,
            ),
            polyalpha_core::MarketTier::Tradeable
        );
        assert_eq!(
            determine_market_tier(
                &settings,
                false,
                polyalpha_core::MarketRetentionReason::HasPosition,
                false,
                None,
                Some(1_995),
                false,
                2_000,
            ),
            polyalpha_core::MarketTier::Focus
        );
        assert_eq!(
            determine_market_tier(
                &settings,
                false,
                polyalpha_core::MarketRetentionReason::None,
                false,
                None,
                Some(1_995),
                false,
                2_000,
            ),
            polyalpha_core::MarketTier::Observation
        );
        assert_eq!(
            determine_market_tier(
                &settings,
                true,
                polyalpha_core::MarketRetentionReason::None,
                true,
                Some(1_990),
                Some(1_995),
                true,
                2_000,
            ),
            polyalpha_core::MarketTier::Focus
        );
    }

    #[test]
    fn next_market_lifecycle_phase_skips_republish_within_same_phase_bucket() {
        let settings = test_settings_with_price_filter(None, None);
        let mut market = settings.markets[0].clone();
        market.settlement_timestamp = 24 * 3600;
        let observed = ObservedState {
            market_phase: Some(MarketPhase::PreSettlement {
                hours_remaining: 20.0,
            }),
            ..ObservedState::default()
        };

        assert_eq!(
            next_market_lifecycle_phase(
                Some(&observed),
                &market,
                market.settlement_timestamp - (19 * 3600),
                &settings.strategy.settlement,
            ),
            None
        );
    }

    #[test]
    fn next_market_lifecycle_phase_publishes_when_phase_bucket_changes() {
        let settings = test_settings_with_price_filter(None, None);
        let mut market = settings.markets[0].clone();
        market.settlement_timestamp = 24 * 3600;
        let observed = ObservedState {
            market_phase: Some(MarketPhase::PreSettlement {
                hours_remaining: 13.0,
            }),
            ..ObservedState::default()
        };

        assert!(matches!(
            next_market_lifecycle_phase(
                Some(&observed),
                &market,
                market.settlement_timestamp - (11 * 3600),
                &settings.strategy.settlement,
            ),
            Some(MarketPhase::ForceReduce { .. })
        ));
    }

    #[tokio::test]
    async fn process_signal_single_rejects_candidate_while_market_pool_cooldown_active() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        let mut observed = ObservedState::default();
        crate::market_pool::maybe_start_open_cooldown(
            &mut observed.market_pool,
            settings.strategy.market_data.max_stale_ms,
            "zero_cex_hedge_qty",
            1,
        );

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_single(
            open_candidate(TokenSide::Yes),
            &settings,
            &registry,
            &mut observed,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            &mut no_audit,
        )
        .await
        .expect("signal should be rejected by market pool gate");

        assert_eq!(stats.signals_rejected, 1);
        assert!(recent_events.iter().any(|event| {
            event.summary.contains("市场处于冷却期") && event.summary.contains("CEX 对冲量为 0")
        }));
    }

    #[tokio::test]
    async fn process_signal_single_cools_market_after_zero_hedge_trade_plan_rejection() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let mut observed = ObservedState::default();
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.delta_estimate = 0.0;

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_single(
            candidate,
            &settings,
            &registry,
            &mut observed,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            &mut no_audit,
        )
        .await
        .expect("signal should reject cleanly");

        assert_eq!(
            observed.market_pool.open_cooldown_reason.as_deref(),
            Some("zero_cex_hedge_qty")
        );
        assert!(observed
            .market_pool
            .open_cooldown_until_ms
            .is_some_and(|until_ms| until_ms > 1));
    }

    #[tokio::test]
    async fn process_signal_single_applies_price_band_before_trade_plan() {
        let settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(5, 1)));
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let mut observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(9, 1)),
            ..ObservedState::default()
        };
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.delta_estimate = 0.0;

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_single(
            candidate,
            &settings,
            &registry,
            &mut observed,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            &mut no_audit,
        )
        .await
        .expect("signal should reject cleanly");

        assert_eq!(
            observed.market_pool.open_cooldown_reason.as_deref(),
            Some("above_or_equal_max_poly_price")
        );
        assert!(recent_events
            .iter()
            .any(|event| { event.summary.contains("高于或等于最大 Poly 价格带") }));
        assert!(!recent_events
            .iter()
            .any(|event| { event.summary.contains("CEX 对冲量为 0") }));
    }

    #[tokio::test]
    async fn process_signal_multi_applies_price_band_before_trade_plan() {
        let settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(5, 1)));
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let symbol = Symbol::new("btc-test");
        let mut observed_map = HashMap::from([(
            symbol.clone(),
            ObservedState {
                poly_yes_mid: Some(Decimal::new(9, 1)),
                ..ObservedState::default()
            },
        )]);
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.delta_estimate = 0.0;

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_multi(
            candidate,
            &settings,
            &registry,
            &mut observed_map,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            false,
            &mut no_audit,
        )
        .await
        .expect("signal should reject cleanly");

        let observed = observed_map.get(&symbol).expect("observed state");
        assert_eq!(
            observed.market_pool.open_cooldown_reason.as_deref(),
            Some("above_or_equal_max_poly_price")
        );
        assert!(recent_events
            .iter()
            .any(|event| { event.summary.contains("高于或等于最大 Poly 价格带") }));
        assert!(!recent_events
            .iter()
            .any(|event| { event.summary.contains("CEX 对冲量为 0") }));
    }

    #[tokio::test]
    async fn process_signal_multi_price_filter_summary_uses_per_asset_band() {
        let settings = test_settings_with_per_asset_basis_overrides();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let symbol = Symbol::new("btc-test");
        let mut observed_map = HashMap::from([(
            symbol.clone(),
            ObservedState {
                poly_yes_mid: Some(Decimal::new(46, 2)),
                ..ObservedState::default()
            },
        )]);
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.symbol = symbol.clone();
        candidate.delta_estimate = 0.0;

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_multi(
            candidate,
            &settings,
            &registry,
            &mut observed_map,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            false,
            &mut no_audit,
        )
        .await
        .expect("signal should reject cleanly");

        let summary = recent_events
            .iter()
            .find(|event| event.summary.contains("高于或等于最大 Poly 价格带"))
            .map(|event| event.summary.as_str())
            .expect("price filter summary");
        assert!(
            summary.contains("0.1 - 0.45"),
            "unexpected summary: {summary}"
        );
    }

    #[tokio::test]
    async fn process_signal_single_cools_market_after_ws_freshness_rejection() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.strategy.market_data.mode = polyalpha_core::MarketDataMode::Ws;
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");

        let now_ms = now_millis();
        let mut observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(45, 2)),
            cex_mid: Some(Decimal::new(100_000, 0)),
            poly_yes_updated_at_ms: Some(now_ms.saturating_sub(100)),
            cex_updated_at_ms: Some(now_ms.saturating_sub(10_000)),
            ..ObservedState::default()
        };
        record_connection_status(
            &mut observed,
            "Polymarket".to_owned(),
            ConnectionStatus::Connected,
            now_ms.saturating_sub(100),
        );
        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            now_ms.saturating_sub(100),
        );

        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut no_audit = None;

        process_signal_single(
            open_candidate(TokenSide::Yes),
            &settings,
            &registry,
            &mut observed,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            true,
            &mut no_audit,
        )
        .await
        .expect("signal should reject cleanly");

        assert_eq!(
            observed.market_pool.open_cooldown_reason.as_deref(),
            Some("cex_stale")
        );
    }

    #[test]
    fn execution_events_include_trade_plan_created_only_when_plan_exists() {
        assert!(!execution_events_include_trade_plan_created(&[
            rejected_order_submitted_event("price_filter"),
        ]));
        assert!(execution_events_include_trade_plan_created(&[
            ExecutionEvent::TradePlanCreated {
                plan: sample_trade_plan(),
            },
        ]));
    }

    #[test]
    fn resolve_artifact_for_read_keeps_legacy_fallback_paper_only() {
        let data_dir = unique_test_data_dir("legacy-paper-fallback");
        let mut settings = test_settings_with_price_filter(None, None);
        settings.general.data_dir = data_dir.clone();

        let paper_dir = PathBuf::from(&data_dir).join("paper");
        fs::create_dir_all(&paper_dir).expect("create paper dir");
        let legacy_path = paper_dir.join("paper-trial-last-session.json");
        fs::write(&legacy_path, "{}").expect("write legacy artifact");

        let paper =
            resolve_artifact_for_read(&settings, "paper-trial", polyalpha_core::TradingMode::Paper)
                .expect("paper artifact path");
        let live =
            resolve_artifact_for_read(&settings, "paper-trial", polyalpha_core::TradingMode::Live)
                .expect("live artifact path");

        assert_eq!(paper, legacy_path);
        assert!(live.ends_with("live/paper-trial-last-run.json"));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn add_trigger_details_formats_structured_residual_snapshot() {
        let intent = PlanningIntent::ResidualRecovery {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-test"),
            residual_snapshot: polyalpha_core::ResidualSnapshot {
                schema_version: PLANNING_SCHEMA_VERSION,
                residual_delta: 0.125,
                planned_cex_qty: polyalpha_core::CexBaseQty(Decimal::new(15, 3)),
                current_poly_yes_shares: PolyShares(Decimal::new(30, 1)),
                current_poly_no_shares: PolyShares(Decimal::new(10, 1)),
                preferred_cex_side: polyalpha_core::OrderSide::Buy,
            },
            recovery_reason: polyalpha_core::RecoveryDecisionReason::CexTopUpFaster,
        };
        let trigger = RuntimeTrigger::Intent {
            intent,
            timestamp_ms: 1,
        };

        let mut details = EventDetails::new();
        add_trigger_details(&mut details, &trigger);

        let snapshot_detail = details
            .get("剩余敞口概况")
            .expect("residual snapshot detail");
        assert!(snapshot_detail.contains("剩余敞口=0.125"));
        assert!(snapshot_detail.contains("优先补对冲方向=买入"));
        assert_eq!(
            details.get("处理原因").map(String::as_str),
            Some("补 CEX 更快")
        );
    }

    #[tokio::test]
    async fn paper_execution_stack_wires_orderbook_provider_into_execution_manager() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());

        let (_provider, _executor, execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");

        assert!(execution.orderbook_provider().is_some());
    }

    #[tokio::test]
    async fn emergency_stop_submits_force_exit_when_position_exists() {
        let mut settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.fair_value = 0.70;
        candidate.raw_mispricing = 0.24;
        candidate.delta_estimate = 0.0005;
        let open_trigger = RuntimeTrigger::Candidate(candidate.clone());
        let open_events = execution
            .process_intent(open_intent_from_candidate(&candidate))
            .await
            .expect("seed open position");
        let mut no_audit = None;
        apply_execution_events(
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            Some(&open_trigger),
            open_events,
            &mut no_audit,
        )
        .await
        .expect("apply open execution events");
        sync_engine_position_state_from_runtime(
            &mut engine,
            &risk,
            &trade_book,
            &market.symbol,
            Some(TokenSide::Yes),
        );
        let open_context = execution
            .planning_context_for_symbol(&market.symbol)
            .expect("open planning context");
        assert!(open_context.current_poly_yes_shares > Decimal::ZERO);
        recent_events.clear();

        let command_center = Arc::new(Mutex::new(CommandCenter::default()));
        command_center.lock().expect("command center").enqueue(
            "cmd-emergency".to_owned(),
            CommandKind::EmergencyStop,
            10_000,
        );
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let pre_emergency_paused = Arc::new(AtomicBool::new(false));

        let _ = process_pending_command_single(
            &command_center,
            &paused,
            &emergency,
            &pre_emergency_paused,
            &mut settings,
            &market,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut no_audit,
            10_000,
        )
        .await
        .expect("process emergency stop");

        let center = command_center.lock().expect("command center");
        let command = center
            .commands
            .get("cmd-emergency")
            .expect("stored command");
        assert_eq!(command.status, CommandStatus::Success);
        assert!(command
            .message
            .as_deref()
            .is_some_and(|message| message.contains("紧急停止已激活")));
        assert!(paused.load(Ordering::SeqCst));
        assert!(emergency.load(Ordering::SeqCst));
        let flattened_context = execution
            .planning_context_for_symbol(&market.symbol)
            .expect("flattened planning context");
        assert_eq!(flattened_context.current_poly_yes_shares, Decimal::ZERO);
        assert_eq!(flattened_context.current_poly_no_shares, Decimal::ZERO);
        assert_eq!(flattened_context.current_cex_net_qty, Decimal::ZERO);
        assert!(!risk
            .position_tracker()
            .symbol_has_open_position(&market.symbol));
    }

    #[tokio::test]
    async fn delta_rebalance_emits_intent_when_threshold_exceeded() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 2);

        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("seed poly fill");
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut no_audit = None;
        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);
        let observed_map = HashMap::from([(
            market.symbol.clone(),
            ObservedState {
                cex_mid: Some(Decimal::new(100_000, 0)),
                ..ObservedState::default()
            },
        )]);
        let now_ms = 70_000;

        maybe_execute_delta_rebalance(
            &settings,
            &observed_map,
            &market.symbol,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut no_audit,
            now_ms,
        )
        .await
        .expect("delta rebalance should execute");

        assert_eq!(
            stats
                .last_delta_rebalance_at_ms
                .get(&market.symbol)
                .copied(),
            Some(now_ms)
        );
        assert!(recent_events.iter().any(|event| {
            event
                .details
                .as_ref()
                .and_then(|details| details.get("动作"))
                .is_some_and(|action| action == "补 CEX 对冲")
        }));
    }

    #[tokio::test]
    async fn delta_rebalance_keeps_symbol_open_when_cex_only_top_up_is_viable() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 2);

        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("seed poly fill");
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut no_audit = None;
        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);

        maybe_execute_delta_rebalance(
            &settings,
            &HashMap::from([(
                market.symbol.clone(),
                ObservedState {
                    cex_mid: Some(Decimal::new(100_000, 0)),
                    ..ObservedState::default()
                },
            )]),
            &market.symbol,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut no_audit,
            70_000,
        )
        .await
        .expect("delta rebalance should execute");

        let plan_event = recent_events
            .iter()
            .find(|event| {
                event.details.as_ref().is_some_and(|details| {
                    details
                        .get("意图")
                        .is_some_and(|intent| intent == "delta_rebalance")
                })
            })
            .expect("delta rebalance trade plan");
        let details = plan_event.details.as_ref().expect("plan details");

        assert!(risk
            .position_tracker()
            .symbol_has_open_position(&market.symbol));
        assert!(!recent_events
            .iter()
            .any(|event| event.summary.contains("已平仓")));
        assert_eq!(details.get("Poly计划股数").map(String::as_str), Some("0"));
        assert!(details
            .get("CEX计划数量")
            .and_then(|qty| Decimal::from_str(qty).ok())
            .is_some_and(|qty| qty > Decimal::ZERO));
    }

    #[tokio::test]
    async fn current_rebalance_snapshot_prefers_cex_buy_when_short_overshoots_target() {
        let settings = test_settings_with_price_filter(None, None);
        let symbol = settings.markets[0].symbol.clone();
        let engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();

        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("seed poly fill");
        risk.on_fill(&Fill {
            quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(30, 1))),
            ..cex_fill(polyalpha_core::OrderSide::Sell)
        })
        .await
        .expect("seed oversized short");
        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);

        let snapshot = current_rebalance_snapshot(&risk, &trade_book, &engine, &symbol)
            .expect("rebalance snapshot should exist");

        assert_eq!(snapshot.preferred_cex_side, OrderSide::Buy);
        assert_eq!(
            snapshot.planned_cex_qty,
            polyalpha_core::CexBaseQty(Decimal::new(25, 1))
        );
        assert_eq!(snapshot.residual_delta, 0.5);
    }

    #[tokio::test]
    async fn current_rebalance_snapshot_uses_last_planned_cex_qty_when_available() {
        let settings = test_settings_with_price_filter(None, None);
        let symbol = settings.markets[0].symbol.clone();
        let engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let mut plan = sample_trade_plan();
        plan.cex_planned_qty = polyalpha_core::CexBaseQty(Decimal::new(777, 3));

        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);
        trade_book.record_trade_plan(&plan);
        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("seed poly fill");

        let snapshot = current_rebalance_snapshot(&risk, &trade_book, &engine, &symbol)
            .expect("rebalance snapshot should exist");

        assert_eq!(snapshot.planned_cex_qty, plan.cex_planned_qty);
        assert_eq!(snapshot.preferred_cex_side, OrderSide::Sell);
    }

    #[tokio::test]
    async fn execute_intent_trigger_keeps_runtime_alive_on_close_rejection() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.fair_value = 0.70;
        candidate.raw_mispricing = 0.24;
        candidate.delta_estimate = 0.0005;
        let open_trigger = RuntimeTrigger::Candidate(candidate.clone());
        let open_events = execution
            .process_intent(open_intent_from_candidate(&candidate))
            .await
            .expect("seed open position");
        let mut no_audit = None;
        apply_execution_events(
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            Some(&open_trigger),
            open_events,
            &mut no_audit,
        )
        .await
        .expect("apply open execution events");
        sync_engine_position_state_from_runtime(
            &mut engine,
            &risk,
            &trade_book,
            &market.symbol,
            Some(TokenSide::Yes),
        );

        update_paper_orderbook(
            &provider,
            &registry,
            &polyalpha_core::OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: market.symbol.clone(),
                instrument: InstrumentKind::PolyYes,
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(44, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(46, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
                }],
                exchange_timestamp_ms: 2_000,
                received_at_ms: 2_100,
                sequence: 2,
                last_trade_price: None,
            },
        );
        recent_events.clear();
        let mut no_audit = None;

        let result = execute_intent_trigger(
            close_intent_for_symbol(&market.symbol, "test_close", "corr-close", 10_000),
            10_000,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut no_audit,
        )
        .await;

        assert!(
            result.is_ok(),
            "close rejection should not abort runtime: {result:?}"
        );
        assert!(recent_events.iter().any(|event| {
            event.summary.contains("Poly 深度不足")
                || event.summary.contains("insufficient_poly_depth")
        }));
    }

    #[tokio::test]
    async fn execute_intent_trigger_records_close_rejection_into_audit() {
        let data_dir = unique_test_data_dir("intent-rejection-audit");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        seed_runtime_books(&provider, &registry, 1);

        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.fair_value = 0.70;
        candidate.raw_mispricing = 0.24;
        candidate.delta_estimate = 0.0005;
        let open_trigger = RuntimeTrigger::Candidate(candidate.clone());
        let mut audit = Some(
            PaperAudit::start(
                &settings,
                "test-env",
                &settings.markets,
                1_000,
                polyalpha_core::TradingMode::Live,
            )
            .expect("start audit"),
        );
        let session_id = audit
            .as_ref()
            .expect("audit session")
            .session_id()
            .to_owned();

        let open_events = execution
            .process_intent(open_intent_from_candidate(&candidate))
            .await
            .expect("seed open position");
        apply_execution_events(
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            Some(&open_trigger),
            open_events,
            &mut audit,
        )
        .await
        .expect("apply open execution events");
        sync_engine_position_state_from_runtime(
            &mut engine,
            &risk,
            &trade_book,
            &market.symbol,
            Some(TokenSide::Yes),
        );

        update_paper_orderbook(
            &provider,
            &registry,
            &polyalpha_core::OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: market.symbol.clone(),
                instrument: InstrumentKind::PolyYes,
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(44, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(46, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
                }],
                exchange_timestamp_ms: 2_000,
                received_at_ms: 2_100,
                sequence: 2,
                last_trade_price: None,
            },
        );
        recent_events.clear();

        execute_intent_trigger(
            close_intent_for_symbol(&market.symbol, "test_close", "corr-close", 10_000),
            10_000,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut audit,
        )
        .await
        .expect("close rejection should be downgraded");
        audit
            .as_mut()
            .expect("audit session")
            .flush()
            .expect("flush audit events");

        let events = polyalpha_audit::AuditReader::load_events(&data_dir, &session_id)
            .expect("load audit events");
        assert!(events.iter().any(|event| {
            matches!(
                &event.payload,
                AuditEventPayload::Anomaly(anomaly)
                    if anomaly.code == "intent_rejected"
                        && anomaly.message.contains("insufficient_poly_depth")
            )
        }));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn finalize_audit_on_runtime_exit_marks_summary_completed() {
        let data_dir = unique_test_data_dir("runtime-exit-finalize");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let mut audit = Some(
            PaperAudit::start(
                &settings,
                "test-env",
                &settings.markets,
                1_000,
                polyalpha_core::TradingMode::Live,
            )
            .expect("start audit"),
        );
        let session_id = audit
            .as_ref()
            .expect("audit session")
            .session_id()
            .to_owned();

        finalize_audit_on_runtime_exit(
            &mut audit,
            &PaperStats::default(),
            &MonitorState::default(),
            3,
            5_000,
        )
        .expect("finalize audit");

        let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
            .expect("load summary");
        assert_eq!(summary.status, AuditSessionStatus::Completed);
        assert_eq!(summary.ended_at_ms, Some(5_000));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn finish_runtime_with_audit_preserves_runtime_error_and_marks_summary_completed() {
        let data_dir = unique_test_data_dir("runtime-exit-finish-error");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let mut audit = Some(
            PaperAudit::start(
                &settings,
                "test-env",
                &settings.markets,
                1_000,
                polyalpha_core::TradingMode::Live,
            )
            .expect("start audit"),
        );
        let session_id = audit
            .as_ref()
            .expect("audit session")
            .session_id()
            .to_owned();

        let err = finish_runtime_with_audit(
            Err(anyhow!("tail failed")),
            &mut audit,
            &PaperStats::default(),
            &MonitorState::default(),
            3,
            5_000,
        )
        .expect_err("should preserve runtime error");

        assert!(err.to_string().contains("tail failed"));
        let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
            .expect("load summary");
        assert_eq!(summary.status, AuditSessionStatus::Completed);
        assert_eq!(summary.ended_at_ms, Some(5_000));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn delta_rebalance_is_throttled_within_interval() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let (_provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("seed poly fill");
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut no_audit = None;
        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);
        let last_ms = 10_000;
        stats
            .last_delta_rebalance_at_ms
            .insert(market.symbol.clone(), last_ms);
        let observed_map = HashMap::from([(
            market.symbol.clone(),
            ObservedState {
                cex_mid: Some(Decimal::new(100_000, 0)),
                ..ObservedState::default()
            },
        )]);

        maybe_execute_delta_rebalance(
            &settings,
            &observed_map,
            &market.symbol,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &mut no_audit,
            last_ms + 1_000,
        )
        .await
        .expect("delta rebalance should be throttled");

        assert_eq!(
            stats
                .last_delta_rebalance_at_ms
                .get(&market.symbol)
                .copied(),
            Some(last_ms)
        );
        assert!(recent_events.is_empty());
    }

    #[tokio::test]
    async fn ws_price_change_keeps_monotonic_poly_book_sequence() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let registry = SymbolRegistry::new(settings.markets.clone());
        let channels = create_channels(std::slice::from_ref(&market.symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let books = Arc::new(Mutex::new(HashMap::new()));
        let diagnostics = PolymarketWsDiagnostics::default();
        let mut market_data_rx = channels.market_data_tx.subscribe();

        handle_polymarket_ws_text(
            &manager,
            &books,
            &diagnostics,
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
        handle_polymarket_ws_text(
            &manager,
            &books,
            &diagnostics,
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
        assert_eq!(
            diagnostics.snapshot(),
            PolymarketWsDiagnosticsSnapshot {
                text_frames: 0,
                price_change_messages: 1,
                book_messages: 1,
                orderbook_updates: 2,
            }
        );
    }

    #[test]
    fn build_multi_monitor_state_from_seed_cache_uses_seeded_market_state() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let mut market_seeds = HashMap::new();
        market_seeds.insert(
            market.symbol.clone(),
            MonitorMarketSeed {
                observed: ObservedState {
                    poly_yes_mid: Some(Decimal::new(48, 2)),
                    poly_no_mid: Some(Decimal::new(52, 2)),
                    cex_mid: Some(Decimal::new(80_000, 0)),
                    poly_yes_updated_at_ms: Some(1_900),
                    cex_updated_at_ms: Some(1_950),
                    connections: HashMap::from([
                        ("Polymarket".to_owned(), ConnectionStatus::Connected),
                        ("Binance".to_owned(), ConnectionStatus::Connected),
                    ]),
                    connection_updated_at_ms: HashMap::from([
                        ("Polymarket".to_owned(), 1_900),
                        ("Binance".to_owned(), 1_950),
                    ]),
                    ..ObservedState::default()
                },
                basis_snapshot: Some(BasisStrategySnapshot {
                    token_side: TokenSide::Yes,
                    poly_price: 0.48,
                    fair_value: 0.51,
                    signal_basis: 0.03,
                    z_score: Some(2.4),
                    delta: 0.12,
                    cex_reference_price: 80_000.0,
                    sigma: Some(0.2),
                    raw_sigma: Some(0.18),
                    sigma_source: "realized".to_owned(),
                    returns_window_len: 90,
                    minutes_to_expiry: 120.0,
                    basis_history_len: 64,
                }),
                basis_history_len: 64,
                has_position: true,
                active_plan_priority: Some("open_position".to_owned()),
            },
        );
        let request = MultiMonitorRenderRequest {
            settings: settings.clone(),
            stats: PaperStats::default(),
            performance: PerformanceMetrics {
                total_pnl_usd: 12.0,
                today_pnl_usd: 3.0,
                win_rate_pct: 50.0,
                max_drawdown_pct: 1.0,
                profit_factor: 1.2,
                equity: 10_012.0,
                initial_capital: 10_000.0,
            },
            position_views: vec![polyalpha_core::PositionView {
                market: market.symbol.0.clone(),
                poly_side: "LONG YES".to_owned(),
                poly_entry_price: 0.45,
                poly_current_price: 0.48,
                poly_quantity: 100.0,
                poly_pnl_usd: 3.0,
                poly_spread_pct: 0.0,
                poly_slippage_bps: 1.0,
                cex_exchange: "Binance".to_owned(),
                cex_side: "SHORT".to_owned(),
                cex_entry_price: 79_000.0,
                cex_current_price: 80_000.0,
                cex_quantity: 0.01,
                cex_pnl_usd: 9.0,
                cex_spread_pct: 0.0,
                cex_slippage_bps: 2.0,
                basis_entry_bps: 120,
                basis_current_bps: 300,
                delta: 0.12,
                hedge_ratio: 1.0,
                total_pnl_usd: 12.0,
                holding_secs: 60,
            }],
            recent_events: Vec::new(),
            recent_trades: Vec::new(),
            recent_commands: Vec::new(),
            paused: false,
            emergency: false,
            start_time_ms: 1_000,
            now_ms: 2_000,
            trading_mode: polyalpha_core::TradingMode::Paper,
            warmup_status_by_symbol: HashMap::new(),
            min_warmup_samples: 30,
            warmup_total_markets: 0,
            warmup_completed_markets: 0,
            warmup_failed_markets: 0,
            has_monitor_clients: true,
            emit_strategy_readiness_alert: false,
            tick_index: 7,
            apply_strategy_health: true,
        };

        let result = build_multi_monitor_state_from_seed_cache(&request, &market_seeds);
        let rendered_market = result
            .monitor_state
            .markets
            .iter()
            .find(|item| item.symbol == market.symbol.0)
            .expect("rendered market");

        assert_eq!(result.tick_index, 7);
        assert!(result.has_monitor_clients);
        assert_eq!(rendered_market.z_score, Some(2.4));
        assert!(rendered_market.has_position);
        assert_eq!(rendered_market.basis_history_len, 64);
        assert_eq!(
            result
                .monitor_state
                .connections
                .get("Binance")
                .and_then(|info| info.updated_at_ms),
            Some(1_950)
        );
    }

    #[test]
    fn extract_plan_rejection_reason_recognizes_revalidation_failure_codes() {
        assert_eq!(
            extract_plan_rejection_reason(
                "plan revalidation failed [poly_price_move_exceeded] and replan rejected [non_positive_planned_edge]: price drift exceeded tolerance"
            ),
            Some("poly_price_move_exceeded".to_owned())
        );
    }

    #[test]
    fn armed_live_requires_confirmation() {
        let err = resolve_live_execution_mode(LiveExecutorMode::Live, false)
            .expect_err("armed live should require confirmation");

        assert!(err.to_string().contains("--confirm-live"));
    }

    #[test]
    fn armed_live_allows_multi_market_runtime_when_confirmed() {
        let mode = resolve_live_execution_mode(LiveExecutorMode::Live, true)
            .expect("armed live should remain available until runtime venue validation");

        assert_eq!(mode, RuntimeExecutionMode::Live);
    }

    #[test]
    fn recovery_plan_payload_surfaces_recovery_reason_code() {
        let plan: TradePlan = serde_json::from_str(
            r#"{
                "schema_version": 1,
                "plan_id": "plan-recovery-1",
                "parent_plan_id": null,
                "supersedes_plan_id": null,
                "idempotency_key": "idem-recovery-1",
                "correlation_id": "corr-1",
                "symbol": "btc-test",
                "intent_type": "residual_recovery",
                "priority": "residual_recovery",
                "created_at_ms": 1,
                "poly_exchange_timestamp_ms": 1,
                "poly_received_at_ms": 1,
                "poly_sequence": 1,
                "cex_exchange_timestamp_ms": 1,
                "cex_received_at_ms": 1,
                "cex_sequence": 1,
                "plan_hash": "hash-recovery-1",
                "poly_side": "Sell",
                "poly_token_side": "Yes",
                "poly_sizing_mode": "sell_exact_shares",
                "poly_requested_shares": "10",
                "poly_planned_shares": "10",
                "poly_max_cost_usd": "0",
                "poly_max_avg_price": "0",
                "poly_max_shares": "0",
                "poly_min_avg_price": "0.42",
                "poly_min_proceeds_usd": "0",
                "poly_book_avg_price": "0.42",
                "poly_executable_price": "0.42",
                "poly_friction_cost_usd": "0.1",
                "poly_fee_usd": "0.02",
                "cex_side": "Buy",
                "cex_planned_qty": "0.005",
                "cex_book_avg_price": "100000",
                "cex_executable_price": "100010",
                "cex_friction_cost_usd": "0.03",
                "cex_fee_usd": "0.01",
                "raw_edge_usd": "1.2",
                "planned_edge_usd": "0.9",
                "expected_funding_cost_usd": "0.01",
                "residual_risk_penalty_usd": "0.02",
                "post_rounding_residual_delta": 0.001,
                "shock_loss_up_1pct": "0.1",
                "shock_loss_down_1pct": "0.1",
                "shock_loss_up_2pct": "0.2",
                "shock_loss_down_2pct": "0.2",
                "plan_ttl_ms": 250,
                "max_poly_sequence_drift": 1,
                "max_cex_sequence_drift": 1,
                "max_poly_price_move": "0.01",
                "max_cex_price_move": "50",
                "min_planned_edge_usd": "0.01",
                "max_residual_delta": 0.01,
                "max_shock_loss_usd": "1.0",
                "max_plan_vs_fill_deviation_usd": "0.5",
                "recovery_decision_reason": "cex_top_up_faster"
            }"#,
        )
        .expect("recovery trade plan");

        let payload =
            execution_event_monitor_payload(&ExecutionEvent::RecoveryPlanCreated { plan }, None)
                .expect("recovery payload");

        let MonitorEventPayload::ExecutionPipeline(view) = payload;
        assert_eq!(
            view.recovery_decision_reason.as_deref(),
            Some("cex_top_up_faster")
        );
    }

    #[test]
    fn append_recent_event_drops_low_priority_noise_when_buffer_is_full_of_high_value_events() {
        let mut events = VecDeque::new();
        for idx in 0..MAX_EVENT_LOGS {
            append_recent_event(
                &mut events,
                test_recent_event("risk", format!("风险事件 {idx}")),
            );
        }

        append_recent_event(
            &mut events,
            test_recent_event("market_phase", "生命周期噪声"),
        );

        assert_eq!(events.len(), MAX_EVENT_LOGS);
        assert!(
            !events.iter().any(|event| event.summary == "生命周期噪声"),
            "低优先级噪声不应挤掉高价值事件"
        );
    }

    #[test]
    fn market_warmup_diagnostic_message_reports_missing_poly_side_and_errors() {
        let market = test_settings_with_price_filter(None, None).markets[0].clone();
        let data = MarketWarmupData {
            cex_klines: vec![(1, 100.0), (2, 101.0), (3, 102.0)],
            poly_histories: vec![(TokenSide::No, vec![(1, 0.36), (2, 0.365)])],
            poly_history_errors: vec![(TokenSide::Yes, "timeout".to_owned())],
        };

        let message = market_warmup_diagnostic_message(&market, &data).expect("diagnostic");

        assert!(message.contains(&market.symbol.0));
        assert!(message.contains("cex=3"));
        assert!(message.contains("yes=0"));
        assert!(message.contains("no=2"));
        assert!(message.contains("YES=timeout"));
    }

    #[test]
    fn warmup_history_fetch_limit_adds_headroom_for_sparse_poly_history() {
        assert_eq!(warmup_history_fetch_limit(600), 750);
        assert_eq!(warmup_history_fetch_limit(100), 160);
        assert_eq!(warmup_history_fetch_limit(1_490), 1_500);
    }

    #[test]
    fn build_strategy_health_summary_counts_warmup_and_history_failures() {
        let markets = vec![
            polyalpha_core::MarketView {
                symbol: "btc-ready".to_owned(),
                z_score: Some(-4.2),
                basis_pct: Some(-1.5),
                poly_price: Some(0.41),
                poly_yes_price: Some(0.41),
                poly_no_price: Some(0.59),
                cex_price: Some(82_000.0),
                fair_value: Some(0.48),
                has_position: false,
                minutes_to_expiry: Some(120.0),
                basis_history_len: 640,
                raw_sigma: None,
                effective_sigma: None,
                sigma_source: None,
                returns_window_len: 0,
                active_token_side: Some("NO".to_owned()),
                poly_updated_at_ms: Some(1),
                cex_updated_at_ms: Some(1),
                poly_quote_age_ms: Some(100),
                cex_quote_age_ms: Some(10),
                cross_leg_skew_ms: Some(90),
                evaluable_status: EvaluableStatus::Evaluable,
                async_classification: AsyncClassification::BalancedFresh,
                market_tier: MarketTier::Tradeable,
                retention_reason: MarketRetentionReason::None,
                last_focus_at_ms: None,
                last_tradeable_at_ms: None,
            },
            polyalpha_core::MarketView {
                symbol: "btc-warmup".to_owned(),
                z_score: None,
                basis_pct: Some(-0.8),
                poly_price: Some(0.33),
                poly_yes_price: Some(0.33),
                poly_no_price: Some(0.67),
                cex_price: Some(82_000.0),
                fair_value: Some(0.41),
                has_position: false,
                minutes_to_expiry: Some(240.0),
                basis_history_len: 128,
                raw_sigma: None,
                effective_sigma: None,
                sigma_source: None,
                returns_window_len: 0,
                active_token_side: Some("NO".to_owned()),
                poly_updated_at_ms: Some(1),
                cex_updated_at_ms: Some(1),
                poly_quote_age_ms: Some(100),
                cex_quote_age_ms: Some(10),
                cross_leg_skew_ms: Some(90),
                evaluable_status: EvaluableStatus::Evaluable,
                async_classification: AsyncClassification::BalancedFresh,
                market_tier: MarketTier::Focus,
                retention_reason: MarketRetentionReason::None,
                last_focus_at_ms: None,
                last_tradeable_at_ms: None,
            },
            polyalpha_core::MarketView {
                symbol: "btc-stale".to_owned(),
                z_score: None,
                basis_pct: Some(-0.3),
                poly_price: Some(0.71),
                poly_yes_price: Some(0.29),
                poly_no_price: Some(0.71),
                cex_price: Some(82_000.0),
                fair_value: Some(0.74),
                has_position: false,
                minutes_to_expiry: Some(300.0),
                basis_history_len: 24,
                raw_sigma: None,
                effective_sigma: None,
                sigma_source: None,
                returns_window_len: 0,
                active_token_side: Some("NO".to_owned()),
                poly_updated_at_ms: Some(1),
                cex_updated_at_ms: Some(1),
                poly_quote_age_ms: Some(8_000),
                cex_quote_age_ms: Some(10),
                cross_leg_skew_ms: Some(7_990),
                evaluable_status: EvaluableStatus::PolyQuoteStale,
                async_classification: AsyncClassification::PolyQuoteStale,
                market_tier: MarketTier::Focus,
                retention_reason: MarketRetentionReason::None,
                last_focus_at_ms: None,
                last_tradeable_at_ms: None,
            },
            polyalpha_core::MarketView {
                symbol: "btc-no-poly".to_owned(),
                z_score: None,
                basis_pct: None,
                poly_price: None,
                poly_yes_price: None,
                poly_no_price: None,
                cex_price: Some(82_000.0),
                fair_value: None,
                has_position: false,
                minutes_to_expiry: None,
                basis_history_len: 0,
                raw_sigma: None,
                effective_sigma: None,
                sigma_source: None,
                returns_window_len: 0,
                active_token_side: None,
                poly_updated_at_ms: None,
                cex_updated_at_ms: Some(1),
                poly_quote_age_ms: None,
                cex_quote_age_ms: Some(10),
                cross_leg_skew_ms: None,
                evaluable_status: EvaluableStatus::NotEvaluableNoPoly,
                async_classification: AsyncClassification::NoPolySample,
                market_tier: MarketTier::Observation,
                retention_reason: MarketRetentionReason::None,
                last_focus_at_ms: None,
                last_tradeable_at_ms: None,
            },
        ];
        let warmup_status = HashMap::from([
            (
                Symbol::new("btc-no-poly"),
                MarketWarmupStatus {
                    cex_fetch_error: Some("cex timeout".to_owned()),
                    ..MarketWarmupStatus::default()
                },
            ),
            (
                Symbol::new("btc-stale"),
                MarketWarmupStatus {
                    poly_history_errors: vec![(TokenSide::Yes, "poly 502".to_owned())],
                    ..MarketWarmupStatus::default()
                },
            ),
        ]);

        let summary = build_strategy_health_summary(&markets, &warmup_status, 600, 4, 3, 1);

        assert_eq!(summary.total_markets, 4);
        assert_eq!(summary.fair_value_ready_markets, 3);
        assert_eq!(summary.z_score_ready_markets, 1);
        assert_eq!(summary.warmup_ready_markets, 1);
        assert_eq!(summary.insufficient_warmup_markets, 2);
        assert_eq!(summary.stale_markets, 1);
        assert_eq!(summary.no_poly_markets, 1);
        assert_eq!(summary.cex_history_failed_markets, 1);
        assert_eq!(summary.poly_history_failed_markets, 1);
        assert_eq!(summary.min_warmup_samples, 600);
        assert_eq!(summary.warmup_total_markets, 4);
        assert_eq!(summary.warmup_completed_markets, 3);
        assert_eq!(summary.warmup_failed_markets, 1);
    }

    #[test]
    fn strategy_readiness_alert_reports_zero_zscore_as_not_ready() {
        let summary = polyalpha_core::StrategyHealthView {
            total_markets: 256,
            fair_value_ready_markets: 194,
            z_score_ready_markets: 0,
            warmup_ready_markets: 12,
            insufficient_warmup_markets: 182,
            stale_markets: 159,
            no_poly_markets: 62,
            cex_history_failed_markets: 0,
            poly_history_failed_markets: 0,
            min_warmup_samples: 600,
            warmup_total_markets: 256,
            warmup_completed_markets: 128,
            warmup_failed_markets: 0,
        };

        let alert = strategy_readiness_alert(&summary, 60_000, 0)
            .expect("zero-zscore state should produce readiness alert");

        assert_eq!(alert.kind, "risk");
        assert!(alert.summary.contains("策略未就绪"));
        assert!(alert.summary.contains("0/256"));
        assert!(alert.summary.contains("预热完成 12/256"));
        assert!(alert.summary.contains("门槛 600"));
    }

    #[test]
    fn build_monitor_events_keeps_rejection_visible_amid_system_noise() {
        let mut events = VecDeque::new();
        for idx in 0..32 {
            append_recent_event(
                &mut events,
                test_recent_event("market_phase", format!("市场阶段噪声 {idx}")),
            );
        }
        append_recent_event(
            &mut events,
            test_recent_event("risk", "信号提交后被拒绝 sig-1（price_filter）"),
        );
        for idx in 32..64 {
            append_recent_event(
                &mut events,
                test_recent_event("market_phase", format!("市场阶段噪声 {idx}")),
            );
        }

        let monitor_events = build_monitor_events(&events);

        assert!(
            monitor_events
                .iter()
                .any(|event| event.summary.contains("被拒绝")),
            "高价值拒绝事件应该保留在 monitor 事件窗口中"
        );
        assert!(
            monitor_events
                .iter()
                .filter(|event| event.kind == EventKind::System)
                .count()
                <= MAX_MONITOR_LOW_PRIORITY_EVENTS
        );
    }

    #[test]
    fn build_monitor_events_pins_recent_trade_closed_events() {
        let mut events = VecDeque::new();
        append_recent_event(&mut events, test_recent_event("trade_closed", "旧平仓 A"));
        append_recent_event(&mut events, test_recent_event("trade_closed", "旧平仓 B"));
        for idx in 0..MAX_MONITOR_EVENTS {
            append_recent_event(
                &mut events,
                test_recent_event("risk", format!("新事件 {idx}")),
            );
        }

        let monitor_events = build_monitor_events(&events);

        assert!(
            monitor_events
                .iter()
                .any(|event| event.summary == "旧平仓 A"),
            "较早的平仓事件也应在 monitor 事件窗口内保留一段时间"
        );
        assert!(
            monitor_events
                .iter()
                .any(|event| event.summary == "旧平仓 B"),
            "最近几笔平仓不应被后续高频事件立即挤掉"
        );
    }

    #[test]
    fn summarize_candidate_uses_chinese_labels() {
        assert_eq!(
            summarize_candidate(&open_candidate(TokenSide::Yes)),
            "开仓机会 cand-1"
        );
    }

    #[test]
    fn summarize_market_event_uses_chinese_labels() {
        let orderbook_event = MarketDataEvent::OrderBookUpdate {
            snapshot: polyalpha_core::OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-test"),
                instrument: InstrumentKind::CexPerp,
                bids: Vec::new(),
                asks: Vec::new(),
                exchange_timestamp_ms: 1,
                received_at_ms: 1,
                sequence: 1,
                last_trade_price: None,
            },
        };
        assert_eq!(
            summarize_market_event(&orderbook_event),
            "订单簿更新 Binance btc-test（交易所永续）"
        );

        let connection_event = MarketDataEvent::ConnectionEvent {
            exchange: Exchange::Polymarket,
            status: ConnectionStatus::Reconnecting,
        };
        assert_eq!(
            summarize_market_event(&connection_event),
            "连接状态更新 Polymarket 重连中"
        );
    }

    #[test]
    fn summarize_execution_event_uses_chinese_labels() {
        assert_eq!(
            summarize_execution_event(&rejected_order_submitted_event("price_filter")),
            "订单已拒绝 Binance ord-1（原因：price_filter）"
        );
        assert_eq!(
            format_ws_freshness_reason("poly_stale"),
            "Polymarket 行情已超时"
        );
    }

    #[test]
    fn format_ws_close_reason_handles_missing_and_explicit_reason() {
        assert_eq!(
            format_ws_close_reason(None),
            "peer closed without frame".to_owned()
        );

        let close_frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
            reason: "heartbeat timeout".into(),
        };
        assert_eq!(
            format_ws_close_reason(Some(&close_frame)),
            "code=1008 reason=heartbeat timeout".to_owned()
        );
    }

    #[test]
    fn build_ws_request_includes_permessage_deflate_header() {
        let request = build_ws_request_with_optional_extensions(
            "wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms",
            Some("permessage-deflate; client_max_window_bits"),
        )
        .expect("ws request");

        assert_eq!(
            request
                .headers()
                .get("Sec-WebSocket-Extensions")
                .and_then(|value| value.to_str().ok()),
            Some("permessage-deflate; client_max_window_bits")
        );
    }

    #[test]
    fn response_negotiated_extensions_extracts_header_value() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(101)
            .header(
                "Sec-WebSocket-Extensions",
                "permessage-deflate; server_no_context_takeover",
            )
            .body(())
            .expect("response");

        assert_eq!(
            negotiated_ws_extensions(&response),
            Some("permessage-deflate; server_no_context_takeover".to_owned())
        );
    }

    #[tokio::test]
    async fn run_ws_future_with_timeout_times_out_slow_connect_attempts() {
        let err = run_ws_future_with_timeout(
            async {
                sleep(Duration::from_millis(50)).await;
                Ok::<(), tokio_tungstenite::tungstenite::Error>(())
            },
            Duration::from_millis(5),
            "polymarket ws connect".to_owned(),
        )
        .await
        .expect_err("slow ws future should time out");

        assert!(err
            .to_string()
            .contains("polymarket ws connect timed out after 5ms"));
    }

    #[test]
    fn ws_connect_delay_ms_is_deterministic_and_spread() {
        let first = ws_connect_delay_ms(1_000, "binance:BTCUSDT");
        let second = ws_connect_delay_ms(1_000, "binance:BTCUSDT");
        let other = ws_connect_delay_ms(1_000, "polymarket:market");

        assert_eq!(first, second);
        assert!(first >= 1_000);
        assert!(first < 3_000);
        assert!(other >= 1_000);
        assert!(other < 3_000);
    }

    #[tokio::test]
    async fn abort_join_handles_aborts_and_drains_spawned_tasks() {
        use std::sync::atomic::AtomicUsize;

        struct DropMarker(Arc<AtomicUsize>);

        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let dropped = dropped.clone();
            handles.push(tokio::spawn(async move {
                let _marker = DropMarker(dropped);
                sleep(Duration::from_secs(60)).await;
            }));
        }
        tokio::task::yield_now().await;

        abort_join_handles(&mut handles).await;

        assert!(handles.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn apply_config_update_accepts_band_policy_disabled() {
        let mut settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(8, 1)));
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let update = polyalpha_core::ConfigUpdate {
            band_policy: Some("disabled".to_owned()),
            min_poly_price: Some(0.0),
            max_poly_price: Some(1.0),
            ..polyalpha_core::ConfigUpdate::default()
        };

        let changed = apply_runtime_config_update(&mut settings, &mut engine, &update)
            .expect("config update");

        assert!(changed.contains("价格带策略=disabled"));
        assert_eq!(
            settings.strategy.basis.band_policy,
            polyalpha_core::BandPolicyMode::Disabled
        );
    }

    #[test]
    fn gate_reason_label_zh_distinguishes_band_policy_rejections() {
        assert_eq!(
            gate_reason_label_zh("price_filter", "below_min_poly_price"),
            "低于最小 Poly 价格带"
        );
        assert_eq!(
            gate_reason_label_zh("price_filter", "above_or_equal_max_poly_price"),
            "高于或等于最大 Poly 价格带"
        );
        assert_eq!(
            gate_reason_label_zh("price_filter", "missing_poly_quote"),
            "缺少 Poly 报价"
        );
    }

    #[test]
    fn translate_trade_plan_rejection_uses_user_facing_open_quality_labels() {
        assert_eq!(
            translate_trade_plan_rejection("open_instant_loss_too_large"),
            "开仓瞬时亏损过大"
        );
        assert_eq!(
            translate_trade_plan_rejection("poly_price_impact_too_large"),
            "Poly 吃单过深，价格冲击过大"
        );
    }

    #[test]
    fn trade_plan_gate_details_include_planning_diagnostics() {
        let settings = test_settings_with_price_filter(None, None);
        let diagnostics = sample_planning_diagnostics();
        let details = build_trade_plan_gate_event_details(
            &open_candidate(TokenSide::No),
            None,
            &settings,
            AuditGateResult::Rejected,
            "poly_price_impact_too_large",
            Some(&diagnostics),
        )
        .expect("trade plan details");

        assert_eq!(
            details.get("Poly可执行价").map(String::as_str),
            Some("0.27864")
        );
        assert_eq!(
            details.get("Poly价格冲击").map(String::as_str),
            Some("320 bps")
        );
        assert_eq!(
            details.get("Poly卖盘Top3").map(String::as_str),
            Some("0.28 x 120 | 0.281 x 90 | 0.282 x 70")
        );
        assert_eq!(
            details.get("开仓瞬时亏损USD").map(String::as_str),
            Some("7.96")
        );
    }

    #[test]
    fn add_trigger_details_uses_user_facing_rebalance_and_residual_labels() {
        let intent = PlanningIntent::ResidualRecovery {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-2".to_owned(),
            correlation_id: "corr-2".to_owned(),
            symbol: Symbol::new("eth-test"),
            residual_snapshot: polyalpha_core::ResidualSnapshot {
                schema_version: PLANNING_SCHEMA_VERSION,
                residual_delta: 0.125,
                planned_cex_qty: polyalpha_core::CexBaseQty(Decimal::new(15, 3)),
                current_poly_yes_shares: PolyShares(Decimal::new(30, 1)),
                current_poly_no_shares: PolyShares(Decimal::new(10, 1)),
                preferred_cex_side: polyalpha_core::OrderSide::Buy,
            },
            recovery_reason: polyalpha_core::RecoveryDecisionReason::CexTopUpFaster,
        };
        let trigger = RuntimeTrigger::Intent {
            intent,
            timestamp_ms: 1,
        };

        let mut details = EventDetails::new();
        add_trigger_details(&mut details, &trigger);

        assert_eq!(
            details.get("动作").map(String::as_str),
            Some("处理剩余敞口")
        );
        assert_eq!(
            details.get("当前流程").map(String::as_str),
            Some("处理剩余敞口")
        );
        assert_eq!(
            details.get("处理原因").map(String::as_str),
            Some("补 CEX 更快")
        );
        let snapshot_detail = details
            .get("剩余敞口概况")
            .expect("residual snapshot detail");
        assert!(snapshot_detail.contains("剩余敞口=0.125"));
        assert!(snapshot_detail.contains("优先补对冲方向=买入"));
    }

    #[test]
    fn paper_audit_runtime_warehouse_sync_error_is_non_fatal() {
        let data_dir = unique_test_data_dir("paper-audit-runtime-sync");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let start_ms = 1_000;
        let mut audit = PaperAudit::start(
            &settings,
            "test-env",
            &settings.markets,
            start_ms,
            polyalpha_core::TradingMode::Paper,
        )
        .expect("start paper audit");
        let session_id = audit.session_id().to_owned();

        break_audit_warehouse_path(&data_dir);

        audit
            .maybe_record_runtime_state(
                &PaperStats::default(),
                &MonitorState::default(),
                1,
                start_ms + 2_000,
            )
            .expect("runtime warehouse sync failure should not abort");
        audit.flush().expect("flush runtime audit state");

        let events = polyalpha_audit::AuditReader::load_events(&data_dir, &session_id)
            .expect("load raw audit events");
        assert!(events.iter().any(|event| {
            matches!(
                &event.payload,
                AuditEventPayload::Anomaly(anomaly)
                    if anomaly.code == "warehouse_sync_degraded"
            )
        }));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn paper_audit_persists_trade_plan_planning_diagnostics() {
        let data_dir = unique_test_data_dir("paper-audit-planning-diagnostics");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let start_ms = 1_000;
        let mut audit = PaperAudit::start(
            &settings,
            "test-env",
            &settings.markets,
            start_ms,
            polyalpha_core::TradingMode::Paper,
        )
        .expect("start paper audit");
        let session_id = audit.session_id().to_owned();
        let diagnostics = sample_planning_diagnostics();

        audit
            .record_gate_decision_with_diagnostics(
                &open_candidate(TokenSide::No),
                "trade_plan",
                AuditGateResult::Rejected,
                "poly_price_impact_too_large",
                "候选被拒绝：未通过交易规划".to_owned(),
                None,
                Some(diagnostics.clone()),
            )
            .expect("record trade plan rejection");

        drop(audit);

        let events = polyalpha_audit::AuditReader::load_events(&data_dir, &session_id)
            .expect("load raw audit events");
        let gate = events
            .iter()
            .find_map(|event| match &event.payload {
                AuditEventPayload::GateDecision(gate)
                    if gate.reason == "poly_price_impact_too_large" =>
                {
                    Some(gate)
                }
                _ => None,
            })
            .expect("gate decision with diagnostics");

        assert_eq!(gate.planning_diagnostics, Some(diagnostics));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn paper_audit_runtime_summary_writes_are_throttled() {
        let data_dir = unique_test_data_dir("paper-audit-summary-throttle");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let start_ms = 1_000;
        let mut audit = PaperAudit::start(
            &settings,
            "test-env",
            &settings.markets,
            start_ms,
            polyalpha_core::TradingMode::Paper,
        )
        .expect("start paper audit");
        let session_id = audit.session_id().to_owned();

        audit
            .maybe_record_runtime_state(
                &PaperStats::default(),
                &MonitorState::default(),
                1,
                start_ms + 200,
            )
            .expect("first runtime state should update in-memory summary");
        audit.flush().expect("flush throttled audit summary");

        let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
            .expect("load throttled summary");
        assert_eq!(summary.latest_tick_index, 0);

        audit
            .maybe_record_runtime_state(
                &PaperStats::default(),
                &MonitorState::default(),
                2,
                start_ms + 1_200,
            )
            .expect("summary write should happen after throttle window");
        audit.flush().expect("flush summary write");

        let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
            .expect("load flushed summary");
        assert_eq!(summary.latest_tick_index, 2);

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn should_build_monitor_state_when_monitor_has_clients() {
        assert!(should_build_monitor_state(1, None, 1_000));
    }

    #[test]
    fn should_build_monitor_state_when_audit_runtime_state_is_due() {
        let data_dir = unique_test_data_dir("paper-audit-needs-runtime-state");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let start_ms = 1_000;
        let audit = PaperAudit::start(
            &settings,
            "test-env",
            &settings.markets,
            start_ms,
            polyalpha_core::TradingMode::Paper,
        )
        .expect("start paper audit");

        assert!(!should_build_monitor_state(0, Some(&audit), start_ms + 200));
        assert!(should_build_monitor_state(
            0,
            Some(&audit),
            start_ms + AUDIT_SUMMARY_WRITE_THROTTLE_MS
        ));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn paper_audit_finalize_warehouse_sync_error_is_non_fatal() {
        let data_dir = unique_test_data_dir("paper-audit-finalize-sync");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let start_ms = 1_000;
        let mut audit = PaperAudit::start(
            &settings,
            "test-env",
            &settings.markets,
            start_ms,
            polyalpha_core::TradingMode::Paper,
        )
        .expect("start paper audit");
        let session_id = audit.session_id().to_owned();

        break_audit_warehouse_path(&data_dir);

        audit
            .finalize(
                &PaperStats::default(),
                &MonitorState::default(),
                1,
                start_ms + 2_000,
            )
            .expect("finalize should keep raw audit artifacts even if warehouse sync fails");

        let summary = polyalpha_audit::AuditReader::load_summary(&data_dir, &session_id)
            .expect("load raw audit summary");
        assert_eq!(summary.status, AuditSessionStatus::Completed);

        let events = polyalpha_audit::AuditReader::load_events(&data_dir, &session_id)
            .expect("load raw audit events");
        assert!(events.iter().any(|event| {
            matches!(
                &event.payload,
                AuditEventPayload::Anomaly(anomaly)
                    if anomaly.code == "warehouse_sync_degraded"
            )
        }));

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn track_open_signal_execution_outcome_pushes_chinese_rejection_summary() {
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let candidate = open_candidate(TokenSide::Yes);
        let mut audit = None;

        track_open_signal_execution_outcome(
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            &candidate,
            &[rejected_order_submitted_event("price_filter")],
            &mut audit,
        )
        .expect("track rejection outcome");

        assert_eq!(stats.signals_rejected, 1);
        assert_eq!(recent_events.len(), 1);
        assert_eq!(recent_events[0].kind, "risk");
        assert_eq!(
            recent_events[0].summary,
            "候选提交后被拒绝 cand-1（price_filter）"
        );
    }

    #[tokio::test]
    async fn resolved_active_token_side_prefers_real_poly_position() {
        let mut risk = InMemoryRiskManager::new(RiskLimits::default());
        risk.on_fill(&poly_fill(TokenSide::Yes))
            .await
            .expect("apply poly fill");

        let mut trade_book = TradeBook::default();
        trade_book.register_open_signal(&open_candidate(TokenSide::No), 0, 1.0, 0.25);

        assert_eq!(
            resolved_active_token_side(
                &risk,
                &trade_book,
                &Symbol::new("btc-test"),
                Some(TokenSide::No),
            ),
            Some(TokenSide::Yes)
        );
    }

    #[tokio::test]
    async fn resolved_active_token_side_falls_back_to_trade_book_for_cex_only_position() {
        let mut risk = InMemoryRiskManager::new(RiskLimits::default());
        risk.on_fill(&cex_fill(polyalpha_core::OrderSide::Sell))
            .await
            .expect("apply cex fill");

        let mut trade_book = TradeBook::default();
        trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);

        assert_eq!(
            resolved_active_token_side(&risk, &trade_book, &Symbol::new("btc-test"), None),
            Some(TokenSide::Yes)
        );
    }

    #[test]
    fn ws_freshness_rejects_stale_poly_leg() {
        let now_ms = now_millis();
        let observed = ObservedState {
            poly_yes_updated_at_ms: Some(now_ms.saturating_sub(7_500)),
            cex_updated_at_ms: Some(now_ms.saturating_sub(100)),
            ..ObservedState::default()
        };
        let reason = ws_candidate_stale_reason(&open_candidate(TokenSide::Yes), &observed, 5_000);
        assert_eq!(reason.as_deref(), Some("poly_stale"));
    }

    #[test]
    fn ws_freshness_accepts_fresh_candidate() {
        let now_ms = now_millis();
        let observed = ObservedState {
            poly_yes_updated_at_ms: Some(now_ms.saturating_sub(100)),
            cex_updated_at_ms: Some(now_ms.saturating_sub(100)),
            ..ObservedState::default()
        };
        assert!(
            ws_candidate_stale_reason(&open_candidate(TokenSide::Yes), &observed, 5_000).is_none()
        );
    }

    #[test]
    fn ws_bootstrap_gate_marks_non_tradeable_market_as_bootstrapping() {
        let observed = AuditObservedMarket {
            symbol: "btc-test".to_owned(),
            evaluable_status: EvaluableStatus::CexQuoteStale,
            async_classification: AsyncClassification::CexQuoteStale,
            ..AuditObservedMarket::default()
        };

        assert_eq!(
            ws_bootstrap_gate_reason(true, Some(&observed)),
            Some("bootstrap_in_progress")
        );
    }

    #[test]
    fn ws_bootstrap_gate_allows_tradeable_market_during_bootstrap() {
        let observed = AuditObservedMarket {
            symbol: "btc-test".to_owned(),
            evaluable_status: EvaluableStatus::Evaluable,
            async_classification: AsyncClassification::BalancedFresh,
            ..AuditObservedMarket::default()
        };

        assert_eq!(ws_bootstrap_gate_reason(true, Some(&observed)), None);
        assert_eq!(ws_bootstrap_gate_reason(false, Some(&observed)), None);
    }

    #[tokio::test]
    async fn build_monitor_state_keeps_realized_pnl_after_position_is_flat() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        risk.on_fill(&cex_fill_with_price(
            polyalpha_core::OrderSide::Buy,
            100_000,
            0,
            1,
        ))
        .await
        .expect("entry fill");
        risk.on_fill(&cex_fill_with_price(
            polyalpha_core::OrderSide::Sell,
            110_000,
            0,
            2,
        ))
        .await
        .expect("exit fill");
        sync_engine_position_state_from_runtime(
            &mut engine,
            &risk,
            &TradeBook::default(),
            &market.symbol,
            Some(TokenSide::Yes),
        );

        let observed = ObservedState::default();
        let mut trade_book = TradeBook::default();
        let command_center = Arc::new(Mutex::new(CommandCenter::default()));
        let execution = ExecutionManager::new(RuntimeExecutor::DryRun(DryRunExecutor::default()));
        let monitor = build_monitor_state(
            &settings,
            &market,
            &engine,
            &observed,
            &PaperStats::default(),
            &risk,
            &execution,
            &mut trade_book,
            &command_center,
            false,
            false,
            0,
            10,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        assert_eq!(
            monitor.positions.len(),
            0,
            "flat positions stay out of the position table"
        );
        assert!(
            (monitor.performance.total_pnl_usd - 10.0).abs() < f64::EPSILON,
            "closed-position realized pnl should still contribute to monitor equity"
        );
    }

    #[test]
    fn build_single_monitor_state_from_seed_cache_matches_legacy_monitor_state() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let engine =
            SimpleAlphaEngine::with_markets(build_paper_engine_config(&settings), settings.markets.clone());
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(48, 2)),
            poly_no_mid: Some(Decimal::new(52, 2)),
            cex_mid: Some(Decimal::new(81_000, 0)),
            poly_yes_updated_at_ms: Some(1_900),
            cex_updated_at_ms: Some(1_950),
            connections: HashMap::from([
                ("Polymarket".to_owned(), ConnectionStatus::Connected),
                ("Binance".to_owned(), ConnectionStatus::Connected),
            ]),
            connection_updated_at_ms: HashMap::from([
                ("Polymarket".to_owned(), 1_900),
                ("Binance".to_owned(), 1_950),
            ]),
            ..ObservedState::default()
        };
        let mut stats = PaperStats::default();
        stats.market_data_rx_lag_events = 2;
        stats.market_data_rx_lagged_messages = 9;
        stats.market_data_tick_drain_last = 3;
        stats.market_data_tick_drain_max = 7;
        stats.polymarket_ws_diagnostics.record_text_frame();
        stats
            .polymarket_ws_diagnostics
            .record_price_change_message();
        stats.polymarket_ws_diagnostics.record_book_message();
        stats.polymarket_ws_diagnostics.add_orderbook_updates(5);

        let risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let execution = ExecutionManager::new(RuntimeExecutor::DryRun(DryRunExecutor::default()));
        let command_center = Arc::new(Mutex::new(CommandCenter::default()));

        let mut legacy_trade_book = TradeBook::default();
        let legacy = build_monitor_state(
            &settings,
            &market,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut legacy_trade_book,
            &command_center,
            false,
            false,
            1_000,
            2_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        let mut reused_trade_book = TradeBook::default();
        let reused = build_single_monitor_state_from_seed_cache(
            &settings,
            &market,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut reused_trade_book,
            &command_center,
            false,
            false,
            1_000,
            2_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        assert_eq!(reused.markets.len(), legacy.markets.len());
        assert_eq!(reused.markets[0].symbol, legacy.markets[0].symbol);
        assert_eq!(reused.markets[0].poly_yes_price, legacy.markets[0].poly_yes_price);
        assert_eq!(reused.markets[0].poly_no_price, legacy.markets[0].poly_no_price);
        assert_eq!(reused.markets[0].cex_price, legacy.markets[0].cex_price);
        assert_eq!(
            reused
                .connections
                .get("Binance")
                .and_then(|info| info.updated_at_ms),
            legacy
                .connections
                .get("Binance")
                .and_then(|info| info.updated_at_ms)
        );
        assert_eq!(
            reused.runtime.market_data_rx_lag_events,
            legacy.runtime.market_data_rx_lag_events
        );
        assert_eq!(
            reused.runtime.polymarket_ws_orderbook_updates,
            legacy.runtime.polymarket_ws_orderbook_updates
        );
        assert_eq!(
            reused.performance.total_pnl_usd,
            legacy.performance.total_pnl_usd
        );
        assert_eq!(reused.positions.len(), legacy.positions.len());
    }

    #[test]
    fn build_single_monitor_state_from_seed_cache_limits_output_to_selected_market() {
        let mut settings = test_settings_with_price_filter(None, None);
        let mut other_market = settings.markets[0].clone();
        other_market.symbol = Symbol::new("eth-test");
        other_market.cex_symbol = "ETHUSDT".to_owned();
        settings.markets.push(other_market);

        let market = settings.markets[0].clone();
        let engine =
            SimpleAlphaEngine::with_markets(build_paper_engine_config(&settings), settings.markets.clone());
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(48, 2)),
            cex_mid: Some(Decimal::new(81_000, 0)),
            ..ObservedState::default()
        };
        let stats = PaperStats::default();
        let risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let execution = ExecutionManager::new(RuntimeExecutor::DryRun(DryRunExecutor::default()));
        let command_center = Arc::new(Mutex::new(CommandCenter::default()));
        let mut trade_book = TradeBook::default();

        let monitor = build_single_monitor_state_from_seed_cache(
            &settings,
            &market,
            &engine,
            &observed,
            &stats,
            &risk,
            &execution,
            &mut trade_book,
            &command_center,
            false,
            false,
            1_000,
            2_000,
            &[],
            polyalpha_core::TradingMode::Paper,
        );

        assert_eq!(monitor.markets.len(), 1);
        assert_eq!(monitor.markets[0].symbol, market.symbol.0);
    }

    #[tokio::test]
    async fn execution_result_funding_adjustment_hits_risk_accounting() {
        let settings = test_settings_with_price_filter(None, None);
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut trade_book = TradeBook::default();
        let mut audit = None;

        apply_execution_events(
            &mut risk,
            &mut stats,
            &mut recent_events,
            &mut trade_book,
            None,
            vec![ExecutionEvent::ExecutionResultRecorded {
                result: sample_execution_result_with_funding(3),
            }],
            &mut audit,
        )
        .await
        .expect("funding adjustment should be applied");

        let snapshot = risk.build_snapshot(10);
        assert_eq!(snapshot.daily_pnl, UsdNotional(Decimal::new(-3, 0)));
        assert_eq!(stats.total_pnl_usd, -3.0);
    }

    #[tokio::test]
    async fn handle_multi_market_event_updates_all_attached_markets_for_cex_venue_orderbook() {
        let mut settings = test_settings_with_price_filter(None, None);
        let mut second_market = settings.markets[0].clone();
        second_market.symbol = Symbol::new("btc-test-2");
        second_market.poly_ids = PolymarketIds {
            condition_id: "condition-2".to_owned(),
            yes_token_id: "yes-2".to_owned(),
            no_token_id: "no-2".to_owned(),
        };
        settings.markets.push(second_market.clone());

        let registry = SymbolRegistry::new(settings.markets.clone());
        let (orderbook_provider, _executor, mut execution) = build_paper_execution_stack(
            &settings,
            &registry,
            1_700_000_000_000,
            RuntimeExecutionMode::Paper,
            None,
        )
        .await
        .expect("paper execution stack");

        let first_symbol = settings.markets[0].symbol.clone();
        let second_symbol = second_market.symbol.clone();
        let mut observed = HashMap::from([
            (first_symbol.clone(), ObservedState::default()),
            (second_symbol.clone(), ObservedState::default()),
        ]);
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut engine = SimpleAlphaEngine::with_markets(
            build_paper_engine_config(&settings),
            settings.markets.clone(),
        );
        let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
        let mut trade_book = TradeBook::default();
        let paused = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));
        let mut audit = None;

        handle_multi_market_event(
            MarketDataEvent::CexVenueOrderBookUpdate {
                exchange: Exchange::Binance,
                venue_symbol: "BTCUSDT".to_owned(),
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(100_000, 0)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 0))),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(100_010, 0)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(1, 0))),
                }],
                exchange_timestamp_ms: 10,
                received_at_ms: 11,
                sequence: 12,
            },
            &settings,
            &registry,
            &orderbook_provider,
            &mut observed,
            &mut stats,
            &mut recent_events,
            &mut engine,
            &mut execution,
            &mut risk,
            &mut trade_book,
            &paused,
            &emergency,
            false,
            &mut audit,
        )
        .await
        .expect("venue-level cex event should be handled");

        assert_eq!(stats.market_events, 1);
        assert_eq!(
            observed
                .get(&first_symbol)
                .and_then(|state| state.cex_mid),
            Some(Decimal::new(100_005, 0))
        );
        assert_eq!(
            observed
                .get(&second_symbol)
                .and_then(|state| state.cex_mid),
            Some(Decimal::new(100_005, 0))
        );

        let first_book = orderbook_provider
            .get_orderbook(Exchange::Binance, &first_symbol, InstrumentKind::CexPerp)
            .expect("first market should resolve shared cex book");
        let second_book = orderbook_provider
            .get_orderbook(Exchange::Binance, &second_symbol, InstrumentKind::CexPerp)
            .expect("second market should resolve shared cex book");

        assert_eq!(first_book.sequence, 12);
        assert_eq!(first_book.symbol, first_symbol);
        assert_eq!(second_book.sequence, 12);
        assert_eq!(second_book.symbol, second_symbol);
    }

    #[test]
    fn observe_market_event_keeps_newer_ws_book_over_late_rest_snapshot() {
        let mut observed = ObservedState::default();
        let symbol = Symbol::new("btc-test");

        observe_market_event(
            &mut observed,
            &MarketDataEvent::OrderBookUpdate {
                snapshot: polyalpha_core::OrderBookSnapshot {
                    exchange: Exchange::Polymarket,
                    symbol: symbol.clone(),
                    instrument: InstrumentKind::PolyYes,
                    bids: vec![polyalpha_core::PriceLevel {
                        price: Price(Decimal::new(48, 2)),
                        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                    }],
                    asks: vec![polyalpha_core::PriceLevel {
                        price: Price(Decimal::new(52, 2)),
                        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                    }],
                    exchange_timestamp_ms: 2_000,
                    received_at_ms: 2_010,
                    sequence: 20,
                    last_trade_price: None,
                },
            },
        );

        observe_market_event(
            &mut observed,
            &MarketDataEvent::OrderBookUpdate {
                snapshot: polyalpha_core::OrderBookSnapshot {
                    exchange: Exchange::Polymarket,
                    symbol,
                    instrument: InstrumentKind::PolyYes,
                    bids: vec![polyalpha_core::PriceLevel {
                        price: Price(Decimal::new(40, 2)),
                        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                    }],
                    asks: vec![polyalpha_core::PriceLevel {
                        price: Price(Decimal::new(44, 2)),
                        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                    }],
                    exchange_timestamp_ms: 1_900,
                    received_at_ms: 2_100,
                    sequence: 0,
                    last_trade_price: None,
                },
            },
        );

        assert_eq!(observed.poly_yes_mid, Some(Decimal::new(50, 2)));
        assert_eq!(observed.poly_yes_updated_at_ms, Some(2_010));
    }

    #[test]
    fn candidate_passes_price_filter_keeps_entry_limits_for_open_signal() {
        let settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(8, 1)));
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(9, 1)),
            ..ObservedState::default()
        };

        assert!(!candidate_passes_price_filter(
            &open_candidate(TokenSide::Yes),
            &observed,
            &settings
        ));
    }

    #[test]
    fn candidate_passes_price_filter_multi_keeps_entry_limits_for_open_signal() {
        let settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(8, 1)));
        let observed_map = HashMap::from([(
            Symbol::new("btc-test"),
            ObservedState {
                poly_yes_mid: Some(Decimal::new(9, 1)),
                ..ObservedState::default()
            },
        )]);
        assert!(!candidate_passes_price_filter_multi(
            &open_candidate(TokenSide::Yes),
            &observed_map,
            &settings
        ));
    }

    #[test]
    fn candidate_passes_price_filter_ignores_limits_when_band_policy_is_disabled() {
        let mut settings =
            test_settings_with_price_filter(Some(Decimal::new(2, 1)), Some(Decimal::new(8, 1)));
        settings.strategy.basis.band_policy = polyalpha_core::BandPolicyMode::Disabled;
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(9, 1)),
            ..ObservedState::default()
        };

        assert!(candidate_passes_price_filter(
            &open_candidate(TokenSide::Yes),
            &observed,
            &settings
        ));
    }

    #[test]
    fn market_open_semantics_guard_rejects_long_horizon_reach_by_date_market() {
        let mut market = test_market("btc-reach-80k-2026", Exchange::Binance, "BTCUSDT");
        market.market_question =
            Some("Will Bitcoin reach $80,000 by December 31, 2026?".to_owned());
        market.market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(80_000, 0))),
            upper_strike: None,
        });
        market.settlement_timestamp = (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);

        assert_eq!(
            market_open_semantics_guard_reason(
                &market,
                &StrategyMarketScopeConfig::default(),
                1_700_000_000_000,
            ),
            Some("path_dependent_market_not_supported")
        );
    }

    #[test]
    fn market_open_semantics_guard_rejects_short_horizon_reach_market() {
        let mut market = test_market("btc-reach-80k-apr", Exchange::Binance, "BTCUSDT");
        market.market_question = Some("Will Bitcoin reach $80,000 March 30-April 5?".to_owned());
        market.market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(80_000, 0))),
            upper_strike: None,
        });
        market.settlement_timestamp = (1_700_000_000_000 / 1_000) + (7 * 24 * 60 * 60);

        assert_eq!(
            market_open_semantics_guard_reason(
                &market,
                &StrategyMarketScopeConfig::default(),
                1_700_000_000_000,
            ),
            Some("path_dependent_market_not_supported")
        );
    }

    #[test]
    fn candidate_price_filter_rejects_unsupported_long_horizon_touch_market() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.markets[0].symbol = Symbol::new("btc-reach-80k-2026");
        settings.markets[0].market_question =
            Some("Will Bitcoin reach $80,000 by December 31, 2026?".to_owned());
        settings.markets[0].market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(80_000, 0))),
            upper_strike: None,
        });
        settings.markets[0].settlement_timestamp =
            (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(4, 1)),
            ..ObservedState::default()
        };
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.symbol = Symbol::new("btc-reach-80k-2026");
        candidate.timestamp_ms = 1_700_000_000_000;

        assert_eq!(
            candidate_price_filter_rejection_reason(&candidate, &observed, &settings),
            Some("path_dependent_market_not_supported")
        );
    }

    #[test]
    fn candidate_price_filter_rejects_unsupported_short_horizon_touch_market() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.markets[0].symbol = Symbol::new("btc-reach-80k-apr");
        settings.markets[0].market_question =
            Some("Will Bitcoin reach $80,000 March 30-April 5?".to_owned());
        settings.markets[0].market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(80_000, 0))),
            upper_strike: None,
        });
        settings.markets[0].settlement_timestamp = (1_700_000_000_000 / 1_000) + (7 * 24 * 60 * 60);
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(4, 1)),
            ..ObservedState::default()
        };
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.symbol = Symbol::new("btc-reach-80k-apr");
        candidate.timestamp_ms = 1_700_000_000_000;

        assert_eq!(
            candidate_price_filter_rejection_reason(&candidate, &observed, &settings),
            Some("path_dependent_market_not_supported")
        );
    }

    #[test]
    fn market_open_semantics_guard_rejects_all_time_high_market() {
        let mut market = test_market("sol-ath-2026", Exchange::Binance, "SOLUSDT");
        market.market_question = Some("Solana all time high by December 31, 2026?".to_owned());
        market.market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(31, 0))),
            upper_strike: None,
        });
        market.settlement_timestamp = (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);

        assert_eq!(
            market_open_semantics_guard_reason(
                &market,
                &StrategyMarketScopeConfig::default(),
                1_700_000_000_000,
            ),
            Some("all_time_high_market_not_supported")
        );
    }

    #[test]
    fn market_open_semantics_guard_rejects_non_price_event_market() {
        let mut market = test_market("btc-sha-2027", Exchange::Binance, "BTCUSDT");
        market.market_question = Some("Will Bitcoin replace SHA-256 before 2027?".to_owned());
        market.market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(256, 0))),
            upper_strike: None,
        });
        market.settlement_timestamp = (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);

        assert_eq!(
            market_open_semantics_guard_reason(
                &market,
                &StrategyMarketScopeConfig::default(),
                1_700_000_000_000,
            ),
            Some("non_price_market_not_supported")
        );
    }

    #[test]
    fn candidate_price_filter_rejects_all_time_high_market() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.markets[0].symbol = Symbol::new("sol-ath-2026");
        settings.markets[0].market_question =
            Some("Solana all time high by December 31, 2026?".to_owned());
        settings.markets[0].market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(31, 0))),
            upper_strike: None,
        });
        settings.markets[0].settlement_timestamp =
            (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(4, 1)),
            ..ObservedState::default()
        };
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.symbol = Symbol::new("sol-ath-2026");
        candidate.timestamp_ms = 1_700_000_000_000;

        assert_eq!(
            candidate_price_filter_rejection_reason(&candidate, &observed, &settings),
            Some("all_time_high_market_not_supported")
        );
    }

    #[test]
    fn candidate_price_filter_rejects_non_price_event_market() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.markets[0].symbol = Symbol::new("btc-sha-2027");
        settings.markets[0].market_question =
            Some("Will Bitcoin replace SHA-256 before 2027?".to_owned());
        settings.markets[0].market_rule = Some(polyalpha_core::MarketRule {
            kind: polyalpha_core::MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(256, 0))),
            upper_strike: None,
        });
        settings.markets[0].settlement_timestamp =
            (1_700_000_000_000 / 1_000) + (31 * 24 * 60 * 60);
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(4, 1)),
            ..ObservedState::default()
        };
        let mut candidate = open_candidate(TokenSide::Yes);
        candidate.symbol = Symbol::new("btc-sha-2027");
        candidate.timestamp_ms = 1_700_000_000_000;

        assert_eq!(
            candidate_price_filter_rejection_reason(&candidate, &observed, &settings),
            Some("non_price_market_not_supported")
        );
    }

    #[test]
    fn market_runtime_expired_settlement_reason_flags_elapsed_market() {
        let mut market = test_market("btc-apr-7", Exchange::Binance, "BTCUSDT");
        market.settlement_timestamp = 1_700_000_000;

        assert_eq!(
            market_runtime_expired_settlement_reason(&market, 1_700_000_000),
            Some("expired_settlement_market")
        );
        assert_eq!(
            market_runtime_expired_settlement_reason(&market, 1_699_999_999),
            None
        );
    }

    #[test]
    fn filter_runtime_markets_for_expired_settlement_removes_elapsed_markets() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.markets = vec![
            test_market("btc-apr-7", Exchange::Binance, "BTCUSDT"),
            test_market("eth-apr-8", Exchange::Binance, "ETHUSDT"),
        ];
        settings.markets[0].settlement_timestamp = 1_700_000_000;
        settings.markets[1].settlement_timestamp = 1_700_086_400;

        let skipped = filter_runtime_markets_for_expired_settlement(&mut settings, 1_700_000_000);

        assert_eq!(skipped, vec![Symbol::new("btc-apr-7")]);
        assert_eq!(
            settings
                .markets
                .iter()
                .map(|market| market.symbol.0.clone())
                .collect::<Vec<_>>(),
            vec!["eth-apr-8".to_owned()]
        );
    }

    #[test]
    fn connection_counters_track_disconnect_and_reconnect() {
        let mut observed = ObservedState::default();

        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            1_000,
        );
        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Reconnecting,
            2_000,
        );
        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            3_000,
        );

        assert_eq!(
            observed.connection_disconnect_count.get("Binance").copied(),
            Some(1)
        );
        assert_eq!(
            observed.connection_reconnect_count.get("Binance").copied(),
            Some(1)
        );
        assert_eq!(
            observed
                .connection_last_disconnect_at_ms
                .get("Binance")
                .copied(),
            Some(2_000)
        );
    }

    #[test]
    fn housekeeping_connection_refresh_does_not_downgrade_connected_ws_state() {
        let mut observed = ObservedState::default();

        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            1_000,
        );

        update_connection_state(
            &mut observed,
            Exchange::Binance,
            ConnectionStatus::Connecting,
            2_000,
        );

        assert_eq!(
            observed.connections.get("Binance"),
            Some(&ConnectionStatus::Connected)
        );
    }

    #[test]
    fn aggregate_connections_keeps_highest_runtime_counters() {
        let mut left = ObservedState::default();
        record_connection_status(
            &mut left,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            1_000,
        );
        record_connection_status(
            &mut left,
            "Binance".to_owned(),
            ConnectionStatus::Reconnecting,
            2_000,
        );
        record_connection_status(
            &mut left,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            3_000,
        );

        let mut right = left.clone();
        record_connection_status(
            &mut right,
            "Binance".to_owned(),
            ConnectionStatus::Reconnecting,
            4_000,
        );

        let observed_map =
            HashMap::from([(Symbol::new("left"), left), (Symbol::new("right"), right)]);
        let aggregated = aggregate_connections(&observed_map, 5_000);
        let info = aggregated.get("Binance").expect("binance connection");

        assert_eq!(info.reconnect_count, 1);
        assert_eq!(info.disconnect_count, 2);
        assert_eq!(info.last_disconnect_at_ms, Some(4_000));
        assert_eq!(info.updated_at_ms, Some(4_000));
        assert_eq!(info.transport_idle_ms, Some(1_000));
    }

    #[test]
    fn market_data_activity_keeps_transport_status_until_explicit_reconnect() {
        let mut observed = ObservedState::default();
        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Connected,
            1_000,
        );
        record_connection_status(
            &mut observed,
            "Binance".to_owned(),
            ConnectionStatus::Reconnecting,
            2_000,
        );

        observe_market_event(
            &mut observed,
            &MarketDataEvent::TradeUpdate {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-test"),
                instrument: InstrumentKind::CexPerp,
                price: Price(Decimal::new(91_500, 0)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(1, 1))),
                side: polyalpha_core::OrderSide::Buy,
                timestamp_ms: 9_000,
            },
        );

        assert_eq!(
            observed.connections.get("Binance"),
            Some(&ConnectionStatus::Reconnecting)
        );
        assert_eq!(
            observed.connection_updated_at_ms.get("Binance"),
            Some(&9_000)
        );
        assert_eq!(
            observed.connection_reconnect_count.get("Binance").copied(),
            None
        );
        assert_eq!(
            observed
                .connection_last_disconnect_at_ms
                .get("Binance")
                .copied(),
            Some(2_000)
        );
    }

    #[test]
    fn missing_connection_status_is_impaired_when_connections_are_required() {
        let settings = test_settings_with_price_filter(None, None);
        let market = settings.markets[0].clone();
        let observed = ObservedState {
            poly_yes_mid: Some(Decimal::new(5, 1)),
            cex_mid: Some(Decimal::new(100_000, 0)),
            poly_yes_updated_at_ms: Some(1_000),
            cex_updated_at_ms: Some(1_050),
            ..ObservedState::default()
        };

        let snapshot = classify_async_market(
            1_500,
            &market,
            &observed,
            freshness_thresholds(
                &settings,
                &effective_basis_config_for_market(&settings, &market),
            ),
            Some(TokenSide::Yes),
            false,
            true,
        );

        assert_eq!(
            snapshot.evaluable_status,
            EvaluableStatus::ConnectionImpaired
        );
        assert_eq!(
            snapshot.async_classification,
            AsyncClassification::ConnectionImpaired
        );
    }

    #[test]
    fn exchange_connection_tracker_mark_connecting_publishes_event() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let symbol = settings.markets[0].symbol.clone();
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let tracker = ExchangeConnectionTracker::new(Exchange::Binance, manager);
        let mut rx = channels.market_data_tx.subscribe();

        tracker.mark_connecting();

        match rx.try_recv() {
            Ok(MarketDataEvent::ConnectionEvent { exchange, status }) => {
                assert_eq!(exchange, Exchange::Binance);
                assert_eq!(status, ConnectionStatus::Connecting);
            }
            other => panic!("unexpected tracker event: {other:?}"),
        }
    }

    #[test]
    fn exchange_connection_tracker_does_not_downgrade_when_other_stream_is_active() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let symbol = settings.markets[0].symbol.clone();
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let tracker = ExchangeConnectionTracker::new(Exchange::Binance, manager);
        let mut rx = channels.market_data_tx.subscribe();

        tracker.mark_connected("btcusdt");
        match rx.try_recv() {
            Ok(MarketDataEvent::ConnectionEvent { exchange, status }) => {
                assert_eq!(exchange, Exchange::Binance);
                assert_eq!(status, ConnectionStatus::Connected);
            }
            other => panic!("unexpected tracker event after connect: {other:?}"),
        }

        tracker.mark_connecting();

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn exchange_connection_tracker_mark_disconnected_requires_all_streams_to_drop() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let symbol = settings.markets[0].symbol.clone();
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let tracker = ExchangeConnectionTracker::new(Exchange::Binance, manager);

        tracker.mark_connected("btcusdt");
        tracker.mark_connected("ethusdt");
        tracker.mark_disconnected("btcusdt");

        assert_eq!(
            tracker.status.lock().map(|guard| *guard).ok(),
            Some(ConnectionStatus::Connected)
        );
    }

    #[test]
    fn binance_transport_mode_uses_combined_when_flag_enabled() {
        let mut settings = test_settings_with_price_filter(None, None);
        settings.strategy.market_data.binance_combined_ws_enabled = true;

        assert_eq!(
            select_binance_ws_transport_mode(&settings),
            BinanceWsTransportMode::Combined
        );
    }

    #[test]
    fn binance_combined_ws_extensions_are_disabled_for_current_client() {
        assert_eq!(binance_combined_ws_requested_extensions(), None);
    }

    #[tokio::test]
    async fn exchange_connection_tracker_reaffirms_connected_state_after_interval() {
        let settings = test_settings_with_price_filter(None, None);
        let registry = SymbolRegistry::new(settings.markets.clone());
        let symbol = settings.markets[0].symbol.clone();
        let channels = create_channels(std::slice::from_ref(&symbol));
        let manager = build_data_manager(&registry, channels.market_data_tx.clone());
        let tracker = ExchangeConnectionTracker::new(Exchange::Polymarket, manager);
        let mut rx = channels.market_data_tx.subscribe();

        tracker.mark_connected("market");
        match rx.try_recv() {
            Ok(MarketDataEvent::ConnectionEvent { exchange, status }) => {
                assert_eq!(exchange, Exchange::Polymarket);
                assert_eq!(status, ConnectionStatus::Connected);
            }
            other => panic!("unexpected tracker event after initial connect: {other:?}"),
        }

        tokio::time::sleep(Duration::from_millis(
            WS_CONNECTED_STATUS_REFRESH_MS.saturating_add(50),
        ))
        .await;
        tracker.reaffirm_connected();

        match rx.try_recv() {
            Ok(MarketDataEvent::ConnectionEvent { exchange, status }) => {
                assert_eq!(exchange, Exchange::Polymarket);
                assert_eq!(status, ConnectionStatus::Connected);
            }
            other => panic!("unexpected tracker event after reaffirm: {other:?}"),
        }
    }

    #[test]
    fn parse_proxy_url_supports_socks5_and_http_connect() {
        assert_eq!(
            parse_proxy_url("socks5://127.0.0.1:7897").expect("parse socks proxy"),
            WsProxyConfig {
                kind: WsProxyKind::Socks5,
                endpoint: WsProxyEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 7897,
                },
            }
        );

        assert_eq!(
            parse_proxy_url("http://127.0.0.1:8080").expect("parse http proxy"),
            WsProxyConfig {
                kind: WsProxyKind::HttpConnect,
                endpoint: WsProxyEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 8080,
                },
            }
        );
    }

    #[test]
    fn parse_scutil_proxy_config_extracts_enabled_endpoints() {
        let proxy = parse_scutil_proxy_config(
            r#"<dictionary> {
  HTTPEnable : 1
  HTTPPort : 7897
  HTTPProxy : 127.0.0.1
  HTTPSEnable : 1
  HTTPSPort : 7898
  HTTPSProxy : 127.0.0.2
  SOCKSEnable : 1
  SOCKSPort : 7899
  SOCKSProxy : 127.0.0.3
}"#,
        );

        assert_eq!(
            proxy,
            SystemProxyConfig {
                socks: Some(WsProxyEndpoint {
                    host: "127.0.0.3".to_owned(),
                    port: 7899,
                }),
                https: Some(WsProxyEndpoint {
                    host: "127.0.0.2".to_owned(),
                    port: 7898,
                }),
                http: Some(WsProxyEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 7897,
                }),
            }
        );
    }

    #[test]
    fn select_ws_proxy_prefers_env_then_system_socks() {
        let system = parse_scutil_proxy_config(
            r#"<dictionary> {
  HTTPEnable : 1
  HTTPPort : 7897
  HTTPProxy : 127.0.0.1
  HTTPSEnable : 1
  HTTPSPort : 7897
  HTTPSProxy : 127.0.0.1
  SOCKSEnable : 1
  SOCKSPort : 7897
  SOCKSProxy : 127.0.0.1
}"#,
        );

        assert_eq!(
            select_ws_proxy("wss", None, None, None, Some(&system)).expect("select system proxy"),
            Some(WsProxyConfig {
                kind: WsProxyKind::Socks5,
                endpoint: WsProxyEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 7897,
                },
            })
        );

        assert_eq!(
            select_ws_proxy(
                "wss",
                Some("http://10.0.0.1:9000"),
                None,
                None,
                Some(&system),
            )
            .expect("select env proxy"),
            Some(WsProxyConfig {
                kind: WsProxyKind::HttpConnect,
                endpoint: WsProxyEndpoint {
                    host: "10.0.0.1".to_owned(),
                    port: 9000,
                },
            })
        );
    }

    #[test]
    fn evaluation_skip_stats_count_attempts_and_debounce_recent_events() {
        let event = cex_evaluation_event();
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();

        let warning = EngineWarning::NoPolyData {
            symbol: Symbol::new("btc-test"),
        };
        let mut audit = None;
        record_evaluation_attempt(
            &mut stats,
            &mut recent_events,
            &event,
            std::slice::from_ref(&warning),
            None,
            &mut audit,
        )
        .expect("record evaluation skip");
        record_evaluation_attempt(
            &mut stats,
            &mut recent_events,
            &event,
            std::slice::from_ref(&warning),
            None,
            &mut audit,
        )
        .expect("record evaluation skip twice");

        assert_eq!(stats.evaluation_attempts, 2);
        assert_eq!(stats.evaluation_skipped, 2);
        assert_eq!(
            stats.skip_reasons.get("no_fresh_poly_sample").copied(),
            Some(2)
        );
        assert_eq!(recent_events.len(), 1);
        assert_eq!(recent_events[0].kind, "skip");
        assert_eq!(
            recent_events[0].summary,
            "btc-test 本轮跳过：暂无新的 Polymarket 行情样本"
        );
    }

    #[test]
    fn evaluation_attempt_without_warning_is_not_counted_as_skip() {
        let event = cex_evaluation_event();
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let mut audit = None;

        record_evaluation_attempt(
            &mut stats,
            &mut recent_events,
            &event,
            &[],
            None,
            &mut audit,
        )
        .expect("record evaluation without warning");

        assert_eq!(stats.evaluation_attempts, 1);
        assert_eq!(stats.evaluable_passes, 1);
        assert_eq!(stats.evaluation_skipped, 0);
        assert!(stats.skip_reasons.is_empty());
        assert!(recent_events.is_empty());
    }

    #[test]
    fn evaluation_skip_audit_records_follow_recent_event_debounce() {
        let data_dir = unique_test_data_dir("evaluation-skip-audit-debounce");
        let settings = test_settings_with_audit_data_dir(data_dir.clone());
        let mut audit = Some(
            PaperAudit::start(
                &settings,
                "test-env",
                &settings.markets,
                1_000,
                polyalpha_core::TradingMode::Live,
            )
            .expect("start audit"),
        );
        let session_id = audit
            .as_ref()
            .expect("audit session")
            .session_id()
            .to_owned();
        let event = cex_evaluation_event();
        let mut stats = PaperStats::default();
        let mut recent_events = VecDeque::new();
        let warning = EngineWarning::NoPolyData {
            symbol: Symbol::new("btc-test"),
        };

        record_evaluation_attempt(
            &mut stats,
            &mut recent_events,
            &event,
            std::slice::from_ref(&warning),
            None,
            &mut audit,
        )
        .expect("record first evaluation skip");
        record_evaluation_attempt(
            &mut stats,
            &mut recent_events,
            &event,
            std::slice::from_ref(&warning),
            None,
            &mut audit,
        )
        .expect("record second evaluation skip");
        audit
            .as_mut()
            .expect("audit session")
            .flush()
            .expect("flush evaluation skip audit");

        let events = polyalpha_audit::AuditReader::load_events(&data_dir, &session_id)
            .expect("load audit events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == AuditEventKind::EvaluationSkip)
                .count(),
            1
        );

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn translate_risk_rejection_handles_zero_cex_hedge_qty() {
        assert_eq!(
            translate_risk_rejection("zero_cex_hedge_qty"),
            "CEX 对冲量为 0，禁止开仓"
        );
    }
}
