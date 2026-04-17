use std::collections::HashMap;

use polyalpha_core::{
    AsyncClassification, ConnectionStatus, EngineWarning, EvaluableStatus, ExecutionEvent,
    MarketView, MonitorState, OpenCandidate, PerformanceMetrics, PlanningDiagnostics, PositionView,
    TradingMode,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditSessionManifest {
    pub version: u32,
    pub session_id: String,
    pub env: String,
    pub mode: TradingMode,
    pub started_at_ms: u64,
    pub market_count: usize,
    pub markets: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditSessionStatus {
    #[default]
    Running,
    Completed,
    Failed,
}

impl AuditSessionStatus {
    pub fn label_zh(self) -> &'static str {
        match self {
            Self::Running => "运行中",
            Self::Completed => "已完成",
            Self::Failed => "失败",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuditCounterSnapshot {
    pub ticks_processed: u64,
    pub market_events: u64,
    pub signals_seen: u64,
    pub signals_rejected: u64,
    pub evaluation_attempts: u64,
    pub evaluable_passes: u64,
    pub evaluation_skipped: u64,
    pub execution_rejected: u64,
    pub order_submitted: u64,
    pub order_cancelled: u64,
    pub fills: u64,
    pub state_changes: u64,
    pub poll_errors: u64,
    pub snapshot_resync_count: u64,
    pub funding_refresh_count: u64,
    pub trades_closed: u64,
    pub market_data_rx_lag_events: u64,
    pub market_data_rx_lagged_messages: u64,
    pub market_data_tick_drain_last: u64,
    pub market_data_tick_drain_max: u64,
    pub polymarket_ws_text_frames: u64,
    pub polymarket_ws_price_change_messages: u64,
    pub polymarket_ws_book_messages: u64,
    pub polymarket_ws_orderbook_updates: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditSessionSummary {
    pub session_id: String,
    pub env: String,
    pub mode: TradingMode,
    pub status: AuditSessionStatus,
    pub started_at_ms: u64,
    #[serde(default)]
    pub ended_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    pub market_count: usize,
    pub markets: Vec<String>,
    pub counters: AuditCounterSnapshot,
    pub latest_tick_index: u64,
    #[serde(default)]
    pub latest_checkpoint_ms: Option<u64>,
    #[serde(default)]
    pub latest_equity: Option<f64>,
    #[serde(default)]
    pub latest_total_pnl_usd: Option<f64>,
    #[serde(default)]
    pub latest_today_pnl_usd: Option<f64>,
    #[serde(default)]
    pub latest_total_exposure_usd: Option<f64>,
    #[serde(default)]
    pub rejection_reasons: HashMap<String, u64>,
    #[serde(default)]
    pub skip_reasons: HashMap<String, u64>,
    #[serde(default)]
    pub evaluable_status_counts: HashMap<String, u64>,
    #[serde(default)]
    pub async_classification_counts: HashMap<String, u64>,
    #[serde(default)]
    pub pulse_session_summaries: Vec<PulseSessionSummaryRow>,
}

impl AuditSessionSummary {
    pub fn new_running(manifest: &AuditSessionManifest) -> Self {
        Self {
            session_id: manifest.session_id.clone(),
            env: manifest.env.clone(),
            mode: manifest.mode,
            status: AuditSessionStatus::Running,
            started_at_ms: manifest.started_at_ms,
            ended_at_ms: None,
            updated_at_ms: manifest.started_at_ms,
            market_count: manifest.market_count,
            markets: manifest.markets.clone(),
            counters: AuditCounterSnapshot::default(),
            latest_tick_index: 0,
            latest_checkpoint_ms: None,
            latest_equity: None,
            latest_total_pnl_usd: None,
            latest_today_pnl_usd: None,
            latest_total_exposure_usd: None,
            rejection_reasons: HashMap::new(),
            skip_reasons: HashMap::new(),
            evaluable_status_counts: HashMap::new(),
            async_classification_counts: HashMap::new(),
            pulse_session_summaries: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditCheckpoint {
    pub session_id: String,
    pub updated_at_ms: u64,
    pub tick_index: u64,
    pub counters: AuditCounterSnapshot,
    pub monitor_state: MonitorState,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuditObservedMarket {
    pub symbol: String,
    #[serde(default)]
    pub poly_yes_mid: Option<f64>,
    #[serde(default)]
    pub poly_no_mid: Option<f64>,
    #[serde(default)]
    pub cex_mid: Option<f64>,
    #[serde(default)]
    pub poly_updated_at_ms: Option<u64>,
    #[serde(default)]
    pub cex_updated_at_ms: Option<u64>,
    #[serde(default)]
    pub poly_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cex_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cross_leg_skew_ms: Option<u64>,
    #[serde(default)]
    pub market_phase: Option<String>,
    #[serde(default)]
    pub connections: HashMap<String, ConnectionStatus>,
    #[serde(default)]
    pub transport_idle_ms_by_connection: HashMap<String, u64>,
    #[serde(default)]
    pub evaluable_status: EvaluableStatus,
    #[serde(default)]
    pub async_classification: AsyncClassification,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditGateResult {
    Passed,
    Rejected,
    Bypassed,
}

impl AuditGateResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Rejected => "rejected",
            Self::Bypassed => "bypassed",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::Passed => "通过",
            Self::Rejected => "拒绝",
            Self::Bypassed => "绕过",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventKind {
    EvaluationSkip,
    SignalEmitted,
    GateDecision,
    Execution,
    MarketMark,
    PositionMark,
    EquityMark,
    Anomaly,
    PulseLifecycle,
    PulseBookTape,
    PulseSessionSummary,
    PulseAssetHealth,
    PulseMarketTape,
    PulseSignalSnapshot,
}

impl AuditEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EvaluationSkip => "evaluation_skip",
            Self::SignalEmitted => "signal_emitted",
            Self::GateDecision => "gate_decision",
            Self::Execution => "execution",
            Self::MarketMark => "market_mark",
            Self::PositionMark => "position_mark",
            Self::EquityMark => "equity_mark",
            Self::Anomaly => "anomaly",
            Self::PulseLifecycle => "pulse_lifecycle",
            Self::PulseBookTape => "pulse_book_tape",
            Self::PulseSessionSummary => "pulse_session_summary",
            Self::PulseAssetHealth => "pulse_asset_health",
            Self::PulseMarketTape => "pulse_market_tape",
            Self::PulseSignalSnapshot => "pulse_signal_snapshot",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::EvaluationSkip => "评估跳过",
            Self::SignalEmitted => "信号发出",
            Self::GateDecision => "闸门决策",
            Self::Execution => "执行事件",
            Self::MarketMark => "市场快照",
            Self::PositionMark => "持仓快照",
            Self::EquityMark => "净值快照",
            Self::Anomaly => "异常",
            Self::PulseLifecycle => "Pulse 生命周期",
            Self::PulseBookTape => "Pulse 盘口 Tape",
            Self::PulseSessionSummary => "Pulse 会话摘要",
            Self::PulseAssetHealth => "Pulse 资产健康",
            Self::PulseMarketTape => "Pulse 市场 Tape",
            Self::PulseSignalSnapshot => "Pulse 信号快照",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationSkipEvent {
    pub source_event: String,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub observed: Option<AuditObservedMarket>,
    #[serde(default)]
    pub warnings: Vec<EngineWarning>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalEmittedEvent {
    pub candidate: OpenCandidate,
    #[serde(default)]
    pub observed: Option<AuditObservedMarket>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateDecisionEvent {
    pub gate: String,
    pub result: AuditGateResult,
    pub reason: String,
    pub candidate: OpenCandidate,
    #[serde(default)]
    pub observed: Option<AuditObservedMarket>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_diagnostics: Option<PlanningDiagnostics>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionAuditEvent {
    pub event: ExecutionEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketMarkEvent {
    pub tick_index: u64,
    pub market: MarketView,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PositionMarkEvent {
    pub tick_index: u64,
    pub position: PositionView,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EquityMarkEvent {
    pub tick_index: u64,
    pub performance: PerformanceMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditAnomalyEvent {
    pub severity: AuditSeverity,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseBookLevelAuditRow {
    pub price: String,
    pub quantity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseBookSnapshotAudit {
    pub exchange: String,
    pub instrument: String,
    pub received_at_ms: u64,
    pub sequence: u64,
    #[serde(default)]
    pub bids: Vec<PulseBookLevelAuditRow>,
    #[serde(default)]
    pub asks: Vec<PulseBookLevelAuditRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseLifecycleAuditEvent {
    pub session_id: String,
    pub asset: String,
    pub state: String,
    #[serde(default)]
    pub entry_price: Option<String>,
    pub planned_poly_qty: String,
    pub actual_poly_filled_qty: String,
    pub actual_poly_fill_ratio: f64,
    pub actual_fill_notional_usd: String,
    #[serde(default)]
    pub candidate_expected_net_pnl_usd: Option<String>,
    pub expected_open_net_pnl_usd: String,
    #[serde(default)]
    pub timeout_loss_estimate_usd: Option<String>,
    #[serde(default)]
    pub required_hit_rate: Option<f64>,
    #[serde(default)]
    pub pulse_score_bps: Option<f64>,
    #[serde(default)]
    pub target_exit_price: Option<String>,
    #[serde(default)]
    pub timeout_exit_price: Option<String>,
    #[serde(default)]
    pub entry_executable_notional_usd: Option<String>,
    #[serde(default)]
    pub reversion_pocket_ticks: Option<f64>,
    #[serde(default)]
    pub reversion_pocket_notional_usd: Option<String>,
    #[serde(default)]
    pub vacuum_ratio: Option<String>,
    pub effective_open: bool,
    pub opening_outcome: String,
    #[serde(default)]
    pub opening_rejection_reason: Option<String>,
    pub opening_allocated_hedge_qty: String,
    pub session_target_delta_exposure: String,
    pub session_allocated_hedge_qty: String,
    pub account_net_target_delta_before_order: String,
    pub account_net_target_delta_after_order: String,
    pub delta_bump_used: String,
    #[serde(default)]
    pub anchor_latency_delta_ms: Option<u64>,
    #[serde(default)]
    pub distance_to_mid_bps: Option<f64>,
    #[serde(default)]
    pub relative_order_age_ms: Option<u64>,
    #[serde(default)]
    pub poly_yes_book: Option<PulseBookSnapshotAudit>,
    #[serde(default)]
    pub poly_no_book: Option<PulseBookSnapshotAudit>,
    #[serde(default)]
    pub cex_book: Option<PulseBookSnapshotAudit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseBookTapeAuditEvent {
    pub session_id: String,
    pub asset: String,
    pub state: String,
    pub symbol: String,
    pub book: PulseBookSnapshotAudit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseMarketTapeAuditEvent {
    pub asset: String,
    pub symbol: String,
    #[serde(alias = "exchange")]
    pub venue: String,
    pub instrument: String,
    #[serde(default)]
    pub token_side: Option<String>,
    #[serde(alias = "exchange_timestamp_ms")]
    pub ts_exchange_ms: u64,
    #[serde(alias = "received_at_ms")]
    pub ts_recv_ms: u64,
    pub sequence: u64,
    #[serde(default = "default_market_update_kind")]
    pub update_kind: String,
    #[serde(default)]
    pub top_n_depth: usize,
    #[serde(default)]
    pub best_bid: Option<String>,
    #[serde(default)]
    pub best_ask: Option<String>,
    #[serde(default)]
    pub mid: Option<String>,
    #[serde(default)]
    pub last_trade_price: Option<String>,
    #[serde(default)]
    pub expiry_ts_ms: Option<u64>,
    #[serde(default)]
    pub strike: Option<String>,
    #[serde(default)]
    pub option_type: Option<String>,
    #[serde(default)]
    pub mark_iv: Option<f64>,
    #[serde(default)]
    pub bid_iv: Option<f64>,
    #[serde(default)]
    pub ask_iv: Option<f64>,
    #[serde(default)]
    pub delta: Option<f64>,
    #[serde(default)]
    pub gamma: Option<f64>,
    #[serde(default)]
    pub index_price: Option<String>,
    #[serde(default)]
    pub delta_bids: Vec<PulseBookLevelAuditRow>,
    #[serde(default)]
    pub delta_asks: Vec<PulseBookLevelAuditRow>,
    #[serde(default, alias = "bids")]
    pub snapshot_bids: Vec<PulseBookLevelAuditRow>,
    #[serde(default, alias = "asks")]
    pub snapshot_asks: Vec<PulseBookLevelAuditRow>,
}

impl PartialEq for PulseMarketTapeAuditEvent {
    fn eq(&self, other: &Self) -> bool {
        self.asset == other.asset
            && self.symbol == other.symbol
            && self.venue == other.venue
            && self.instrument == other.instrument
            && self.token_side == other.token_side
            && self.ts_exchange_ms == other.ts_exchange_ms
            && self.ts_recv_ms == other.ts_recv_ms
            && self.sequence == other.sequence
            && self.update_kind == other.update_kind
            && self.top_n_depth == other.top_n_depth
            && self.best_bid == other.best_bid
            && self.best_ask == other.best_ask
            && self.mid == other.mid
            && self.last_trade_price == other.last_trade_price
            && self.expiry_ts_ms == other.expiry_ts_ms
            && self.strike == other.strike
            && self.option_type == other.option_type
            && self.mark_iv == other.mark_iv
            && self.bid_iv == other.bid_iv
            && self.ask_iv == other.ask_iv
            && self.delta == other.delta
            && self.gamma == other.gamma
            && self.index_price == other.index_price
            && book_levels_equal(&self.delta_bids, &other.delta_bids)
            && book_levels_equal(&self.delta_asks, &other.delta_asks)
            && book_levels_equal(&self.snapshot_bids, &other.snapshot_bids)
            && book_levels_equal(&self.snapshot_asks, &other.snapshot_asks)
    }
}

impl Eq for PulseMarketTapeAuditEvent {}

fn default_market_update_kind() -> String {
    "snapshot".to_owned()
}

fn book_levels_equal(
    left: &[PulseBookLevelAuditRow],
    right: &[PulseBookLevelAuditRow],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left_level, right_level)| {
                left_level.price == right_level.price
                    && left_level.quantity == right_level.quantity
            })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PulseSignalMode {
    ElasticSnapback,
    DeepReversion,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PulseSignalSnapshotAuditEvent {
    pub asset: String,
    pub symbol: String,
    pub mode_candidate: PulseSignalMode,
    pub admission_result: AuditGateResult,
    #[serde(default)]
    pub rejection_reason: Option<String>,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub claim_side: Option<String>,
    #[serde(default)]
    pub fair_prob_yes: Option<f64>,
    #[serde(default)]
    pub entry_price: Option<String>,
    #[serde(default)]
    pub net_edge_bps: Option<f64>,
    #[serde(default)]
    pub expected_net_pnl_usd: Option<String>,
    #[serde(default)]
    pub target_exit_price: Option<String>,
    #[serde(default)]
    pub timeout_exit_price: Option<String>,
    #[serde(default)]
    pub timeout_loss_estimate_usd: Option<String>,
    pub pulse_score_bps: f64,
    pub claim_price_move_bps: f64,
    pub fair_claim_move_bps: f64,
    pub cex_mid_move_bps: f64,
    pub swept_notional_usd: String,
    pub swept_levels_count: usize,
    pub post_pulse_depth_gap_bps: f64,
    pub min_profitable_target_distance_bps: f64,
    pub reachability_cap_bps: f64,
    pub in_gray_zone: bool,
    pub reachable: bool,
    #[serde(default)]
    pub target_distance_to_mid_bps: Option<f64>,
    #[serde(default)]
    pub predicted_hit_rate: Option<f64>,
    #[serde(default)]
    pub maker_net_pnl_usd: Option<String>,
    #[serde(default)]
    pub timeout_net_pnl_usd: Option<String>,
    #[serde(default)]
    pub realizable_ev_usd: Option<String>,
    pub used_exchange_ts: bool,
    pub native_sequence_present: bool,
    pub post_sweep_update_count_5s: usize,
    pub max_interarrival_gap_ms_5s: u64,
    #[serde(default)]
    pub observation_quality_score: Option<f64>,
    pub admission_eligible: bool,
    #[serde(default)]
    pub anchor_age_ms: Option<u64>,
    #[serde(default)]
    pub anchor_latency_delta_ms: Option<u64>,
    #[serde(default)]
    pub anchor_expiry_mismatch_minutes: Option<i64>,
    #[serde(default)]
    pub yes_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub no_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cex_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub poly_transport_healthy: Option<bool>,
    #[serde(default)]
    pub hedge_transport_healthy: Option<bool>,
    #[serde(default)]
    pub poly_yes_sequence_ref: Option<u64>,
    #[serde(default)]
    pub poly_no_sequence_ref: Option<u64>,
    #[serde(default)]
    pub cex_sequence_ref: Option<u64>,
    #[serde(default)]
    pub anchor_sequence_ref: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PulseExecutionRouteContext {
    pub asset: String,
    pub anchor_provider: String,
    pub hedge_venue: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PulseExecutionContext {
    pub session_id: String,
    pub env: String,
    pub mode: String,
    #[serde(default)]
    pub enabled_assets: Vec<String>,
    #[serde(default)]
    pub market_symbols: Vec<String>,
    #[serde(default)]
    pub routes: Vec<PulseExecutionRouteContext>,
    pub poly_fee_bps: u32,
    pub cex_taker_fee_bps: u32,
    pub cex_slippage_bps: u32,
    pub maker_proxy_version: String,
    pub max_holding_secs: u64,
    pub opening_request_notional_usd: String,
    pub min_opening_notional_usd: String,
    pub min_expected_net_pnl_usd: String,
    pub require_nonzero_hedge: bool,
    #[serde(default)]
    pub min_open_fill_ratio: Option<String>,
}

impl PulseExecutionContext {
    pub fn hedge_venue_for_asset(&self, asset: &str) -> Option<&str> {
        self.routes
            .iter()
            .find(|route| route.asset.eq_ignore_ascii_case(asset))
            .map(|route| route.hedge_venue.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseSessionSummaryRow {
    pub pulse_session_id: String,
    pub asset: String,
    pub state: String,
    pub opened_at_ms: u64,
    #[serde(default)]
    pub closed_at_ms: Option<u64>,
    pub planned_poly_qty: String,
    pub actual_poly_filled_qty: String,
    pub actual_poly_fill_ratio: f64,
    #[serde(default)]
    pub entry_price: Option<String>,
    pub actual_fill_notional_usd: String,
    #[serde(default)]
    pub candidate_expected_net_pnl_usd: Option<String>,
    pub expected_open_net_pnl_usd: String,
    #[serde(default)]
    pub timeout_loss_estimate_usd: Option<String>,
    #[serde(default)]
    pub required_hit_rate: Option<f64>,
    #[serde(default)]
    pub pulse_score_bps: Option<f64>,
    pub effective_open: bool,
    pub opening_outcome: String,
    #[serde(default)]
    pub opening_rejection_reason: Option<String>,
    pub opening_allocated_hedge_qty: String,
    pub session_target_delta_exposure: String,
    pub session_allocated_hedge_qty: String,
    #[serde(default)]
    pub net_edge_bps: Option<f64>,
    #[serde(default)]
    pub realized_pnl_usd: Option<f64>,
    #[serde(default)]
    pub exit_path: Option<String>,
    #[serde(default)]
    pub target_exit_price: Option<String>,
    #[serde(default)]
    pub final_exit_price: Option<String>,
    #[serde(default)]
    pub timeout_exit_price: Option<String>,
    #[serde(default)]
    pub entry_executable_notional_usd: Option<String>,
    #[serde(default)]
    pub reversion_pocket_ticks: Option<f64>,
    #[serde(default)]
    pub reversion_pocket_notional_usd: Option<String>,
    #[serde(default)]
    pub vacuum_ratio: Option<String>,
    #[serde(default)]
    pub anchor_latency_delta_ms: Option<u64>,
    #[serde(default)]
    pub distance_to_mid_bps: Option<f64>,
    #[serde(default)]
    pub relative_order_age_ms: Option<u64>,
}

pub type PulseSessionSummaryEvent = PulseSessionSummaryRow;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseAssetHealthAuditEvent {
    pub asset: String,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub anchor_age_ms: Option<u64>,
    #[serde(default)]
    pub anchor_latency_delta_ms: Option<u64>,
    #[serde(default)]
    pub poly_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub cex_quote_age_ms: Option<u64>,
    #[serde(default)]
    pub open_sessions: usize,
    #[serde(default)]
    pub net_target_delta: Option<String>,
    #[serde(default)]
    pub actual_exchange_position: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub disable_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditEventPayload {
    EvaluationSkip(EvaluationSkipEvent),
    SignalEmitted(SignalEmittedEvent),
    GateDecision(GateDecisionEvent),
    Execution(ExecutionAuditEvent),
    MarketMark(MarketMarkEvent),
    PositionMark(PositionMarkEvent),
    EquityMark(EquityMarkEvent),
    Anomaly(AuditAnomalyEvent),
    PulseLifecycle(PulseLifecycleAuditEvent),
    PulseBookTape(PulseBookTapeAuditEvent),
    PulseSessionSummary(PulseSessionSummaryEvent),
    PulseAssetHealth(PulseAssetHealthAuditEvent),
    PulseMarketTape(PulseMarketTapeAuditEvent),
    PulseSignalSnapshot(PulseSignalSnapshotAuditEvent),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewAuditEvent {
    pub timestamp_ms: u64,
    pub kind: AuditEventKind,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub gate: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub summary: String,
    pub payload: AuditEventPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub session_id: String,
    pub env: String,
    pub seq: u64,
    pub timestamp_ms: u64,
    pub kind: AuditEventKind,
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub gate: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub summary: String,
    pub payload: AuditEventPayload,
}

#[cfg(test)]
mod tests {
    use super::{
        AuditEventPayload, AuditGateResult, AuditSessionSummary, PulseBookLevelAuditRow,
        PulseBookSnapshotAudit, PulseBookTapeAuditEvent, PulseExecutionContext,
        PulseExecutionRouteContext, PulseLifecycleAuditEvent, PulseMarketTapeAuditEvent,
        PulseSignalMode, PulseSignalSnapshotAuditEvent,
    };

    #[test]
    fn session_summary_deserializes_with_legacy_counter_fields_missing() {
        let raw = r#"
        {
          "session_id": "legacy-session",
          "env": "multi-market-active",
          "mode": "Paper",
          "status": "running",
          "started_at_ms": 1774417042627,
          "ended_at_ms": null,
          "updated_at_ms": 1774424364040,
          "market_count": 1,
          "markets": ["eth-reach-2400-mar"],
          "counters": {
            "ticks_processed": 7090,
            "market_events": 1223406,
            "signals_seen": 1,
            "signals_rejected": 1,
            "evaluation_attempts": 875369,
            "evaluation_skipped": 696,
            "order_submitted": 0,
            "order_cancelled": 0,
            "fills": 0,
            "state_changes": 0,
            "poll_errors": 0,
            "snapshot_resync_count": 24,
            "funding_refresh_count": 121,
            "trades_closed": 0
          },
          "latest_tick_index": 7090,
          "latest_checkpoint_ms": 1774424363041,
          "latest_equity": 10000.0,
          "latest_total_pnl_usd": 0.0,
          "latest_today_pnl_usd": 0.0,
          "latest_total_exposure_usd": 0.0,
          "rejection_reasons": {
            "price_filter": 1
          },
          "skip_reasons": {
            "no_fresh_poly_sample": 696
          }
        }
        "#;

        let summary: AuditSessionSummary =
            serde_json::from_str(raw).expect("legacy summary should deserialize");

        assert_eq!(summary.counters.evaluation_attempts, 875369);
        assert_eq!(summary.counters.evaluation_skipped, 696);
        assert_eq!(summary.counters.evaluable_passes, 0);
        assert_eq!(summary.counters.execution_rejected, 0);
        assert_eq!(summary.counters.trades_closed, 0);
    }

    #[test]
    fn pulse_session_record_serializes_partial_fill_and_hedge_attribution() {
        let payload = AuditEventPayload::PulseLifecycle(PulseLifecycleAuditEvent {
            session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            entry_price: Some("0.35".to_owned()),
            planned_poly_qty: "10000".to_owned(),
            actual_poly_filled_qty: "3500".to_owned(),
            actual_poly_fill_ratio: 0.35,
            actual_fill_notional_usd: "1225".to_owned(),
            candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
            expected_open_net_pnl_usd: "3.85".to_owned(),
            timeout_loss_estimate_usd: Some("21.68".to_owned()),
            required_hit_rate: Some(0.768),
            pulse_score_bps: Some(182.5),
            target_exit_price: Some("0.38".to_owned()),
            timeout_exit_price: Some("0.31".to_owned()),
            entry_executable_notional_usd: Some("250".to_owned()),
            reversion_pocket_ticks: Some(4.0),
            reversion_pocket_notional_usd: Some("28.57".to_owned()),
            vacuum_ratio: Some("1".to_owned()),
            effective_open: true,
            opening_outcome: "effective_open".to_owned(),
            opening_rejection_reason: None,
            opening_allocated_hedge_qty: "0.39".to_owned(),
            session_target_delta_exposure: "0.41".to_owned(),
            session_allocated_hedge_qty: "0.39".to_owned(),
            account_net_target_delta_before_order: "0.52".to_owned(),
            account_net_target_delta_after_order: "0.13".to_owned(),
            delta_bump_used: "10".to_owned(),
            anchor_latency_delta_ms: None,
            distance_to_mid_bps: None,
            relative_order_age_ms: None,
            poly_yes_book: None,
            poly_no_book: None,
            cex_book: None,
        });

        let encoded = serde_json::to_string(&payload).expect("serialize payload");
        assert!(encoded.contains("\"pulse_lifecycle\""));
        assert!(encoded.contains("\"actual_poly_filled_qty\":\"3500\""));
        assert!(encoded.contains("\"opening_outcome\":\"effective_open\""));
        assert!(encoded.contains("\"timeout_loss_estimate_usd\":\"21.68\""));
        assert!(encoded.contains("\"required_hit_rate\":0.768"));
    }

    #[test]
    fn pulse_book_tape_record_serializes_raw_book_snapshot() {
        let payload = AuditEventPayload::PulseBookTape(PulseBookTapeAuditEvent {
            session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            symbol: "btc-above-100k".to_owned(),
            book: PulseBookSnapshotAudit {
                exchange: "Binance".to_owned(),
                instrument: "cex_perp".to_owned(),
                received_at_ms: 1_717_171_717_123,
                sequence: 42,
                bids: vec![PulseBookLevelAuditRow {
                    price: "100000".to_owned(),
                    quantity: "1.5".to_owned(),
                }],
                asks: vec![PulseBookLevelAuditRow {
                    price: "100010".to_owned(),
                    quantity: "1.2".to_owned(),
                }],
            },
        });

        let encoded = serde_json::to_string(&payload).expect("serialize tape payload");
        assert!(encoded.contains("\"pulse_book_tape\""));
        assert!(encoded.contains("\"instrument\":\"cex_perp\""));
        assert!(encoded.contains("\"sequence\":42"));
    }

    #[test]
    fn pulse_market_tape_serializes_sequence_and_level_deltas() {
        let payload = AuditEventPayload::PulseMarketTape(PulseMarketTapeAuditEvent {
            asset: "btc".to_owned(),
            symbol: "btc-above-100k".to_owned(),
            venue: "Polymarket".to_owned(),
            instrument: "poly_no".to_owned(),
            token_side: Some("no".to_owned()),
            ts_exchange_ms: 1_710_000_100,
            ts_recv_ms: 1_710_000_140,
            sequence: 42,
            update_kind: "snapshot".to_owned(),
            top_n_depth: 5,
            best_bid: Some("0.38".to_owned()),
            best_ask: Some("0.39".to_owned()),
            mid: Some("0.385".to_owned()),
            last_trade_price: Some("0.39".to_owned()),
            expiry_ts_ms: None,
            strike: None,
            option_type: None,
            mark_iv: None,
            bid_iv: None,
            ask_iv: None,
            delta: None,
            gamma: None,
            index_price: None,
            delta_bids: vec![PulseBookLevelAuditRow {
                price: "0.38".to_owned(),
                quantity: "25".to_owned(),
            }],
            delta_asks: vec![PulseBookLevelAuditRow {
                price: "0.39".to_owned(),
                quantity: "30".to_owned(),
            }],
            snapshot_bids: vec![
                PulseBookLevelAuditRow {
                    price: "0.38".to_owned(),
                    quantity: "250".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.37".to_owned(),
                    quantity: "200".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.36".to_owned(),
                    quantity: "180".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.35".to_owned(),
                    quantity: "160".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.34".to_owned(),
                    quantity: "140".to_owned(),
                },
            ],
            snapshot_asks: vec![
                PulseBookLevelAuditRow {
                    price: "0.39".to_owned(),
                    quantity: "300".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.40".to_owned(),
                    quantity: "260".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.41".to_owned(),
                    quantity: "220".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.42".to_owned(),
                    quantity: "190".to_owned(),
                },
                PulseBookLevelAuditRow {
                    price: "0.43".to_owned(),
                    quantity: "170".to_owned(),
                },
            ],
        });

        let json = serde_json::to_string(&payload).expect("serialize");
        let decoded: AuditEventPayload = serde_json::from_str(&json).expect("deserialize");
        let reencoded = serde_json::to_string(&decoded).expect("re-serialize");
        assert_eq!(reencoded, json);
        let event = match decoded {
            AuditEventPayload::PulseMarketTape(event) => event,
            _ => panic!("decoded payload should be PulseMarketTape"),
        };
        assert_eq!(event.update_kind, "snapshot");
        assert_eq!(event.top_n_depth, 5);
        assert_eq!(event.delta_bids.len(), 1);
        assert_eq!(event.delta_asks.len(), 1);
        assert_eq!(event.snapshot_bids.len(), 5);
        assert_eq!(event.snapshot_asks.len(), 5);
        let bid_levels = event
            .snapshot_bids
            .iter()
            .map(|level| (level.price.as_str(), level.quantity.as_str()))
            .collect::<Vec<_>>();
        let ask_levels = event
            .snapshot_asks
            .iter()
            .map(|level| (level.price.as_str(), level.quantity.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            bid_levels,
            vec![
                ("0.38", "250"),
                ("0.37", "200"),
                ("0.36", "180"),
                ("0.35", "160"),
                ("0.34", "140"),
            ]
        );
        assert_eq!(
            ask_levels,
            vec![
                ("0.39", "300"),
                ("0.40", "260"),
                ("0.41", "220"),
                ("0.42", "190"),
                ("0.43", "170"),
            ]
        );
    }

    #[test]
    fn pulse_signal_snapshot_serializes_pricing_and_exit_fields() {
        let payload = AuditEventPayload::PulseSignalSnapshot(PulseSignalSnapshotAuditEvent {
            asset: "eth".to_owned(),
            symbol: "eth-above-4k".to_owned(),
            mode_candidate: PulseSignalMode::ElasticSnapback,
            admission_result: AuditGateResult::Rejected,
            rejection_reason: Some("realizable_ev_below_threshold".to_owned()),
            provider_id: Some("deribit_primary".to_owned()),
            claim_side: Some("no".to_owned()),
            fair_prob_yes: Some(0.61),
            entry_price: Some("0.35".to_owned()),
            net_edge_bps: Some(118.0),
            expected_net_pnl_usd: Some("4.72".to_owned()),
            target_exit_price: Some("0.38".to_owned()),
            timeout_exit_price: Some("0.31".to_owned()),
            timeout_loss_estimate_usd: Some("21.68".to_owned()),
            pulse_score_bps: 188.4,
            claim_price_move_bps: 206.0,
            fair_claim_move_bps: 15.0,
            cex_mid_move_bps: 21.0,
            swept_notional_usd: "420".to_owned(),
            swept_levels_count: 3,
            post_pulse_depth_gap_bps: 145.0,
            min_profitable_target_distance_bps: 92.0,
            reachability_cap_bps: 130.0,
            in_gray_zone: false,
            reachable: true,
            target_distance_to_mid_bps: Some(118.0),
            predicted_hit_rate: Some(0.61),
            maker_net_pnl_usd: Some("4.72".to_owned()),
            timeout_net_pnl_usd: Some("-3.10".to_owned()),
            realizable_ev_usd: Some("1.62".to_owned()),
            used_exchange_ts: true,
            native_sequence_present: true,
            post_sweep_update_count_5s: 7,
            max_interarrival_gap_ms_5s: 120,
            observation_quality_score: Some(0.86),
            admission_eligible: true,
            anchor_age_ms: Some(45),
            anchor_latency_delta_ms: Some(18),
            anchor_expiry_mismatch_minutes: Some(240),
            yes_quote_age_ms: Some(15),
            no_quote_age_ms: Some(12),
            cex_quote_age_ms: Some(8),
            poly_transport_healthy: Some(true),
            hedge_transport_healthy: Some(true),
            poly_yes_sequence_ref: Some(101),
            poly_no_sequence_ref: Some(102),
            cex_sequence_ref: Some(77),
            anchor_sequence_ref: Some(1_710_000_123),
        });

        let json = serde_json::to_string(&payload).expect("serialize");
        let decoded: AuditEventPayload = serde_json::from_str(&json).expect("deserialize");
        let reencoded = serde_json::to_string(&decoded).expect("re-serialize");
        assert_eq!(reencoded, json);
        let event = match decoded {
            AuditEventPayload::PulseSignalSnapshot(event) => event,
            _ => panic!("decoded payload should be PulseSignalSnapshot"),
        };
        assert_eq!(event.mode_candidate, PulseSignalMode::ElasticSnapback);
        assert_eq!(event.admission_result, AuditGateResult::Rejected);
        assert_eq!(
            event.rejection_reason.as_deref(),
            Some("realizable_ev_below_threshold")
        );
        assert_eq!(event.fair_prob_yes, Some(0.61));
        assert_eq!(event.entry_price.as_deref(), Some("0.35"));
        assert_eq!(event.net_edge_bps, Some(118.0));
        assert_eq!(event.expected_net_pnl_usd.as_deref(), Some("4.72"));
        assert_eq!(event.target_exit_price.as_deref(), Some("0.38"));
        assert_eq!(event.timeout_exit_price.as_deref(), Some("0.31"));
        assert_eq!(event.timeout_loss_estimate_usd.as_deref(), Some("21.68"));
        assert_eq!(event.reachability_cap_bps, 130.0);
        assert!(!event.in_gray_zone);
        assert!(event.reachable);
        assert_eq!(event.target_distance_to_mid_bps, Some(118.0));
        assert_eq!(event.predicted_hit_rate, Some(0.61));
        assert!(event.used_exchange_ts);
        assert!(event.native_sequence_present);
        assert_eq!(event.post_sweep_update_count_5s, 7);
        assert_eq!(event.max_interarrival_gap_ms_5s, 120);
        assert_eq!(event.observation_quality_score, Some(0.86));
        assert_eq!(event.anchor_latency_delta_ms, Some(18));
        assert_eq!(event.poly_yes_sequence_ref, Some(101));
        assert_eq!(event.poly_no_sequence_ref, Some(102));
        assert_eq!(event.cex_sequence_ref, Some(77));
        assert_eq!(event.anchor_sequence_ref, Some(1_710_000_123));
        assert!(event.admission_eligible);
    }

    #[test]
    fn pulse_execution_context_serializes_static_runtime_constraints() {
        let context = PulseExecutionContext {
            session_id: "session-1".to_owned(),
            env: "multi-market-active".to_owned(),
            mode: "live_mock".to_owned(),
            enabled_assets: vec!["btc".to_owned(), "eth".to_owned()],
            market_symbols: vec!["btc-above-100k".to_owned(), "eth-above-4k".to_owned()],
            routes: vec![
                PulseExecutionRouteContext {
                    asset: "btc".to_owned(),
                    anchor_provider: "deribit_primary".to_owned(),
                    hedge_venue: "binance_usdm_perp".to_owned(),
                },
                PulseExecutionRouteContext {
                    asset: "eth".to_owned(),
                    anchor_provider: "deribit_primary".to_owned(),
                    hedge_venue: "binance_usdm_perp".to_owned(),
                },
            ],
            poly_fee_bps: 100,
            cex_taker_fee_bps: 5,
            cex_slippage_bps: 2,
            maker_proxy_version: "book_proxy_v1".to_owned(),
            max_holding_secs: 900,
            opening_request_notional_usd: "250".to_owned(),
            min_opening_notional_usd: "50".to_owned(),
            min_expected_net_pnl_usd: "0.5".to_owned(),
            require_nonzero_hedge: true,
            min_open_fill_ratio: Some("0.05".to_owned()),
        };

        let json = serde_json::to_string(&context).expect("serialize execution context");
        let decoded: PulseExecutionContext =
            serde_json::from_str(&json).expect("deserialize execution context");

        assert_eq!(decoded.mode, "live_mock");
        assert_eq!(decoded.routes.len(), 2);
        assert_eq!(decoded.routes[0].anchor_provider, "deribit_primary");
        assert_eq!(decoded.hedge_venue_for_asset("eth"), Some("binance_usdm_perp"));
        assert_eq!(decoded.min_open_fill_ratio.as_deref(), Some("0.05"));
        assert!(decoded.require_nonzero_hedge);
    }
}
