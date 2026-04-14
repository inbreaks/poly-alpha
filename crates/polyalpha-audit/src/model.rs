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
    pub planned_poly_qty: String,
    pub actual_poly_filled_qty: String,
    pub actual_poly_fill_ratio: f64,
    pub actual_fill_notional_usd: String,
    pub expected_open_net_pnl_usd: String,
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
    pub actual_fill_notional_usd: String,
    pub expected_open_net_pnl_usd: String,
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
        AuditEventPayload, AuditSessionSummary, PulseBookLevelAuditRow, PulseBookSnapshotAudit,
        PulseBookTapeAuditEvent, PulseLifecycleAuditEvent,
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
            planned_poly_qty: "10000".to_owned(),
            actual_poly_filled_qty: "3500".to_owned(),
            actual_poly_fill_ratio: 0.35,
            actual_fill_notional_usd: "1225".to_owned(),
            expected_open_net_pnl_usd: "3.85".to_owned(),
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
}
