//! Monitor 消息协议定义
//!
//! 用于 Paper Trading 进程和 Monitor 进程之间的通信。

use crate::ConnectionStatus;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 通信消息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    /// 服务端 → 客户端：状态更新
    StateUpdate {
        timestamp_ms: u64,
        state: MonitorState,
    },

    /// 服务端 → 客户端：事件推送
    Event {
        timestamp_ms: u64,
        event: MonitorEvent,
    },

    /// 客户端 → 服务端：命令
    Command {
        command_id: String,
        kind: CommandKind,
    },

    /// 服务端 → 客户端：命令确认
    CommandAck {
        command_id: String,
        kind: CommandKind,
        status: CommandStatus,
        success: bool,
        message: Option<String>,
        error_code: Option<u32>,
        finished: bool,
        timed_out: bool,
        cancellable: bool,
    },
}

/// 监控状态
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonitorState {
    pub timestamp_ms: u64,
    pub mode: TradingMode,
    pub uptime_secs: u64,
    pub paused: bool,
    pub emergency: bool,

    pub performance: PerformanceMetrics,
    pub positions: Vec<PositionView>,
    pub markets: Vec<MarketView>,
    pub signals: SignalStats,
    #[serde(default)]
    pub evaluation: EvaluationStats,
    pub config: MonitorStrategyConfig,
    #[serde(default)]
    pub runtime: MonitorRuntimeStats,
    pub connections: HashMap<String, ConnectionInfo>,
    pub recent_events: Vec<MonitorEvent>,
    #[serde(default)]
    pub recent_trades: Vec<TradeView>,
    #[serde(default)]
    pub recent_commands: Vec<CommandView>,
}

/// 交易模式
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradingMode {
    #[default]
    Paper,
    Live,
    Backtest,
}

/// 当前市场是否具备评估条件
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluableStatus {
    #[default]
    Unknown,
    Evaluable,
    NotEvaluableNoPoly,
    NotEvaluableNoCex,
    NotEvaluableMisaligned,
    ConnectionImpaired,
    PolyQuoteStale,
    CexQuoteStale,
}

impl EvaluableStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Evaluable => "evaluable",
            Self::NotEvaluableNoPoly => "not_evaluable_no_poly",
            Self::NotEvaluableNoCex => "not_evaluable_no_cex",
            Self::NotEvaluableMisaligned => "not_evaluable_misaligned",
            Self::ConnectionImpaired => "connection_impaired",
            Self::PolyQuoteStale => "poly_quote_stale",
            Self::CexQuoteStale => "cex_quote_stale",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::Unknown => "未知",
            Self::Evaluable => "可评估",
            Self::NotEvaluableNoPoly => "缺少新的 Polymarket 样本",
            Self::NotEvaluableNoCex => "缺少可用的交易所参考价",
            Self::NotEvaluableMisaligned => "双腿时间未对齐",
            Self::ConnectionImpaired => "行情连接异常",
            Self::PolyQuoteStale => "Polymarket 报价已超时",
            Self::CexQuoteStale => "交易所报价已超时",
        }
    }
}

impl MonitorState {
    /// Apply a client-side market filter while preserving global runtime/performance context.
    pub fn filter_market(&self, market: &str) -> Self {
        let mut filtered = self.clone();
        filtered.positions = self
            .positions
            .iter()
            .filter(|position| position.market == market)
            .cloned()
            .collect();
        filtered.markets = self
            .markets
            .iter()
            .filter(|item| item.symbol == market)
            .cloned()
            .collect();
        filtered.recent_events = self
            .recent_events
            .iter()
            .filter(|event| event.matches_market(market))
            .cloned()
            .collect();
        filtered.recent_trades = self
            .recent_trades
            .iter()
            .filter(|trade| trade.market == market)
            .cloned()
            .collect();
        filtered.recent_commands = self
            .recent_commands
            .iter()
            .filter(|command| command.kind.matches_market(market))
            .cloned()
            .collect();
        filtered
    }
}

/// 异步行情分类
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AsyncClassification {
    #[default]
    Unknown,
    BalancedFresh,
    PolyLagAcceptable,
    PolyLagBorderline,
    NoPolySample,
    NoCexReference,
    PolyQuoteStale,
    CexQuoteStale,
    Misaligned,
    ConnectionImpaired,
}

impl AsyncClassification {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::BalancedFresh => "balanced_fresh",
            Self::PolyLagAcceptable => "poly_lag_acceptable",
            Self::PolyLagBorderline => "poly_lag_borderline",
            Self::NoPolySample => "no_poly_sample",
            Self::NoCexReference => "no_cex_reference",
            Self::PolyQuoteStale => "poly_quote_stale",
            Self::CexQuoteStale => "cex_quote_stale",
            Self::Misaligned => "misaligned",
            Self::ConnectionImpaired => "connection_impaired",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::Unknown => "未知",
            Self::BalancedFresh => "双腿新鲜",
            Self::PolyLagAcceptable => "慢腿可交易",
            Self::PolyLagBorderline => "慢腿临界",
            Self::NoPolySample => "缺少新的 Polymarket 样本",
            Self::NoCexReference => "缺少可用的交易所参考价",
            Self::PolyQuoteStale => "Polymarket 报价已超时",
            Self::CexQuoteStale => "交易所报价已超时",
            Self::Misaligned => "双腿时间未对齐",
            Self::ConnectionImpaired => "行情连接异常",
        }
    }
}

/// 业绩指标
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_pnl_usd: f64,
    pub today_pnl_usd: f64,
    pub win_rate_pct: f64,
    pub max_drawdown_pct: f64,
    pub profit_factor: f64,
    pub equity: f64,
    pub initial_capital: f64,
}

/// 持仓视图（两腿）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PositionView {
    pub market: String,

    // Poly 腿
    pub poly_side: String,
    pub poly_entry_price: f64,
    pub poly_current_price: f64,
    pub poly_quantity: f64,
    pub poly_pnl_usd: f64,
    pub poly_spread_pct: f64,
    pub poly_slippage_bps: f64,

    // CEX 腿
    pub cex_exchange: String,
    pub cex_side: String,
    pub cex_entry_price: f64,
    pub cex_current_price: f64,
    pub cex_quantity: f64,
    pub cex_pnl_usd: f64,
    pub cex_spread_pct: f64,
    pub cex_slippage_bps: f64,

    // 配对指标
    pub basis_entry_bps: i32,
    pub basis_current_bps: i32,
    pub delta: f64,
    pub hedge_ratio: f64,
    pub total_pnl_usd: f64,
    pub holding_secs: u64,
}

/// 市场视图
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketView {
    pub symbol: String,
    pub z_score: Option<f64>,
    pub basis_pct: Option<f64>,
    #[serde(default)]
    pub poly_price: Option<f64>,
    #[serde(default)]
    pub poly_yes_price: Option<f64>,
    #[serde(default)]
    pub poly_no_price: Option<f64>,
    pub cex_price: Option<f64>,
    pub fair_value: Option<f64>,
    pub has_position: bool,
    pub minutes_to_expiry: Option<f64>,
    #[serde(default)]
    pub active_token_side: Option<String>,
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
    pub evaluable_status: EvaluableStatus,
    #[serde(default)]
    pub async_classification: AsyncClassification,
}

/// 信号统计
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SignalStats {
    pub seen: u64,
    pub rejected: u64,
    pub rejection_reasons: HashMap<String, u64>,
}

/// 评估统计
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EvaluationStats {
    #[serde(default)]
    pub attempts: u64,
    #[serde(default)]
    pub evaluable_passes: u64,
    #[serde(default)]
    pub skipped: u64,
    #[serde(default)]
    pub execution_rejected: u64,
    #[serde(default)]
    pub skip_reasons: HashMap<String, u64>,
    #[serde(default)]
    pub evaluable_status_counts: HashMap<String, u64>,
    #[serde(default)]
    pub async_classification_counts: HashMap<String, u64>,
}

/// 策略配置（用于 Monitor 显示）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorStrategyConfig {
    pub entry_z: f64,
    pub exit_z: f64,
    pub position_notional_usd: f64,
    pub rolling_window: u64,
    #[serde(default)]
    pub market_data_mode: String,
    #[serde(default)]
    pub max_stale_ms: u64,
    #[serde(default)]
    pub poly_open_max_quote_age_ms: u64,
    #[serde(default)]
    pub cex_open_max_quote_age_ms: u64,
    #[serde(default)]
    pub close_max_quote_age_ms: u64,
    #[serde(default)]
    pub max_cross_leg_skew_ms: u64,
    #[serde(default)]
    pub borderline_poly_quote_age_ms: u64,
    pub min_poly_price: f64,
    pub max_poly_price: f64,
    pub poly_slippage_bps: u64,
    pub cex_slippage_bps: u64,
    #[serde(default)]
    pub max_total_exposure_usd: f64,
    #[serde(default)]
    pub max_single_position_usd: f64,
    #[serde(default)]
    pub max_daily_loss_usd: f64,
    #[serde(default)]
    pub max_open_orders: usize,
    #[serde(default)]
    pub rate_limit_orders_per_sec: u64,
    #[serde(default)]
    pub min_liquidity: f64,
    #[serde(default)]
    pub allow_partial_fill: bool,
    #[serde(default)]
    pub enable_freshness_check: bool,
    #[serde(default)]
    pub reject_on_disconnect: bool,
}

impl Default for MonitorStrategyConfig {
    fn default() -> Self {
        Self {
            entry_z: 4.0,
            exit_z: 0.5,
            position_notional_usd: 10000.0,
            rolling_window: 600,
            market_data_mode: "poll".to_owned(),
            max_stale_ms: 0,
            poly_open_max_quote_age_ms: 0,
            cex_open_max_quote_age_ms: 0,
            close_max_quote_age_ms: 0,
            max_cross_leg_skew_ms: 0,
            borderline_poly_quote_age_ms: 0,
            min_poly_price: 0.20,
            max_poly_price: 0.50,
            poly_slippage_bps: 50,
            cex_slippage_bps: 2,
            max_total_exposure_usd: 50_000.0,
            max_single_position_usd: 10_000.0,
            max_daily_loss_usd: 2_000.0,
            max_open_orders: 10,
            rate_limit_orders_per_sec: 5,
            min_liquidity: 100.0,
            allow_partial_fill: false,
            enable_freshness_check: true,
            reject_on_disconnect: true,
        }
    }
}

/// 运行时指标
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonitorRuntimeStats {
    #[serde(default)]
    pub snapshot_resync_count: u64,
    #[serde(default)]
    pub funding_refresh_count: u64,
}

/// 连接信息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub status: ConnectionStatus,
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: Option<u64>,
    #[serde(default)]
    pub transport_idle_ms: Option<u64>,
    #[serde(default)]
    pub reconnect_count: u64,
    #[serde(default)]
    pub disconnect_count: u64,
    #[serde(default)]
    pub last_disconnect_at_ms: Option<u64>,
}

/// 已关闭交易视图
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradeView {
    pub market: String,
    pub opened_at_ms: u64,
    pub closed_at_ms: u64,
    pub direction: String,
    pub token_side: String,
    pub entry_price: Option<f64>,
    pub exit_price: Option<f64>,
    pub realized_pnl_usd: f64,
    pub correlation_id: Option<String>,
}

/// 命令状态
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandStatus {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
    PartialSuccess,
}

/// 最近命令视图
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandView {
    pub command_id: String,
    pub kind: CommandKind,
    pub status: CommandStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished: bool,
    pub timed_out: bool,
    pub cancellable: bool,
    pub message: Option<String>,
    pub error_code: Option<u32>,
}

/// 监控事件
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorEvent {
    pub timestamp_ms: u64,
    pub kind: EventKind,
    pub market: Option<String>,
    pub correlation_id: Option<String>,
    pub summary: String,
    pub details: Option<HashMap<String, String>>,
}

impl MonitorEvent {
    pub fn matches_market(&self, market: &str) -> bool {
        self.market.as_deref() == Some(market)
            || self.summary.contains(market)
            || self
                .correlation_id
                .as_deref()
                .is_some_and(|id| id.contains(market))
            || self.details.as_ref().is_some_and(|details| {
                details
                    .iter()
                    .any(|(key, value)| key.contains(market) || value.contains(market))
            })
    }
}

/// 事件类型
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventKind {
    Signal,
    Gate,
    Trade,
    TradeClosed,
    Skip,
    Risk,
    System,
    Error,
}

/// 命令类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandKind {
    /// 暂停交易
    Pause,
    /// 恢复交易
    Resume,
    /// 平仓
    ClosePosition { market: String },
    /// 平仓全部
    CloseAllPositions,
    /// 紧急停止
    EmergencyStop,
    /// 解除紧急停止
    ClearEmergency,
    /// 更新配置
    UpdateConfig { config: ConfigUpdate },
    /// 取消命令
    CancelCommand { command_id: String },
}

impl CommandKind {
    pub fn matches_market(&self, market: &str) -> bool {
        match self {
            Self::ClosePosition {
                market: command_market,
            } => command_market == market,
            Self::Pause
            | Self::Resume
            | Self::CloseAllPositions
            | Self::EmergencyStop
            | Self::ClearEmergency
            | Self::UpdateConfig { .. }
            | Self::CancelCommand { .. } => true,
        }
    }
}

/// 配置更新
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConfigUpdate {
    pub entry_z: Option<f64>,
    pub exit_z: Option<f64>,
    pub position_notional_usd: Option<f64>,
    pub rolling_window: Option<u64>,
    pub min_poly_price: Option<f64>,
    pub max_poly_price: Option<f64>,
}

impl Message {
    /// 序列化为 JSON 字节
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let json = serde_json::to_string(self)?;
        let mut bytes = json.into_bytes();
        bytes.push(b'\n'); // 添加换行符作为消息分隔符
        Ok(bytes)
    }

    /// 从 JSON 字节解析
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_message_serialization() {
        let msg = Message::StateUpdate {
            timestamp_ms: 1234567890,
            state: MonitorState::default(),
        };

        let bytes = msg.to_bytes().unwrap();
        let decoded = Message::from_bytes(&bytes[..bytes.len() - 1]).unwrap();

        match decoded {
            Message::StateUpdate { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 1234567890);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_command_serialization() {
        let msg = Message::Command {
            command_id: "test-123".to_string(),
            kind: CommandKind::ClosePosition {
                market: "btc-100k-mar28".to_string(),
            },
        };

        let bytes = msg.to_bytes().unwrap();
        let decoded = Message::from_bytes(&bytes[..bytes.len() - 1]).unwrap();

        match decoded {
            Message::Command { command_id, kind } => {
                assert_eq!(command_id, "test-123");
                match kind {
                    CommandKind::ClosePosition { market } => {
                        assert_eq!(market, "btc-100k-mar28");
                    }
                    _ => panic!("Wrong command kind"),
                }
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn monitor_state_deserializes_when_evaluation_stats_are_missing() {
        let raw = json!({
            "timestamp_ms": 1,
            "mode": "Paper",
            "uptime_secs": 2,
            "paused": false,
            "emergency": false,
            "performance": {
                "total_pnl_usd": 0.0,
                "today_pnl_usd": 0.0,
                "win_rate_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "profit_factor": 0.0,
                "equity": 10000.0,
                "initial_capital": 10000.0
            },
            "positions": [],
            "markets": [],
            "signals": {
                "seen": 0,
                "rejected": 0,
                "rejection_reasons": {}
            },
            "config": {
                "entry_z": 4.0,
                "exit_z": 0.5,
                "position_notional_usd": 10000.0,
                "rolling_window": 600,
                "min_poly_price": 0.2,
                "max_poly_price": 0.5,
                "poly_slippage_bps": 50,
                "cex_slippage_bps": 2
            },
            "connections": {},
            "recent_events": [],
            "recent_trades": [],
            "recent_commands": []
        });

        let state: MonitorState = serde_json::from_value(raw).unwrap();
        assert_eq!(state.evaluation.attempts, 0);
        assert_eq!(state.evaluation.skipped, 0);
        assert!(state.evaluation.skip_reasons.is_empty());
    }

    #[test]
    fn filter_market_keeps_requested_market_and_related_items() {
        let mut state = MonitorState::default();
        state.positions = vec![
            PositionView {
                market: "sol-dip-80-mar".to_owned(),
                poly_side: "LONG YES".to_owned(),
                poly_entry_price: 0.23,
                poly_current_price: 0.24,
                poly_quantity: 100.0,
                poly_pnl_usd: 1.0,
                poly_spread_pct: 0.0,
                poly_slippage_bps: 50.0,
                cex_exchange: "Binance".to_owned(),
                cex_side: "SHORT".to_owned(),
                cex_entry_price: 90.0,
                cex_current_price: 89.5,
                cex_quantity: 1.0,
                cex_pnl_usd: 0.5,
                cex_spread_pct: 0.0,
                cex_slippage_bps: 2.0,
                basis_entry_bps: 100,
                basis_current_bps: 120,
                delta: -0.1,
                hedge_ratio: 1.0,
                total_pnl_usd: 1.5,
                holding_secs: 60,
            },
            PositionView {
                market: "eth-reach-2400-mar".to_owned(),
                ..PositionView {
                    market: String::new(),
                    poly_side: "LONG YES".to_owned(),
                    poly_entry_price: 0.25,
                    poly_current_price: 0.26,
                    poly_quantity: 100.0,
                    poly_pnl_usd: 1.0,
                    poly_spread_pct: 0.0,
                    poly_slippage_bps: 50.0,
                    cex_exchange: "Binance".to_owned(),
                    cex_side: "SHORT".to_owned(),
                    cex_entry_price: 2150.0,
                    cex_current_price: 2145.0,
                    cex_quantity: 1.0,
                    cex_pnl_usd: 0.5,
                    cex_spread_pct: 0.0,
                    cex_slippage_bps: 2.0,
                    basis_entry_bps: 100,
                    basis_current_bps: 120,
                    delta: -0.1,
                    hedge_ratio: 1.0,
                    total_pnl_usd: 1.5,
                    holding_secs: 60,
                }
            },
        ];
        state.markets = vec![
            MarketView {
                symbol: "sol-dip-80-mar".to_owned(),
                z_score: Some(-2.1),
                basis_pct: Some(16.7),
                poly_price: Some(0.23),
                poly_yes_price: Some(0.23),
                poly_no_price: Some(0.77),
                cex_price: Some(91.4),
                fair_value: Some(0.065),
                has_position: true,
                minutes_to_expiry: Some(100.0),
                active_token_side: Some("YES".to_owned()),
                poly_updated_at_ms: Some(1),
                cex_updated_at_ms: Some(2),
                poly_quote_age_ms: Some(10),
                cex_quote_age_ms: Some(20),
                cross_leg_skew_ms: Some(10),
                evaluable_status: EvaluableStatus::Evaluable,
                async_classification: AsyncClassification::BalancedFresh,
            },
            MarketView {
                symbol: "eth-reach-2400-mar".to_owned(),
                z_score: Some(-0.3),
                basis_pct: Some(10.0),
                poly_price: Some(0.25),
                poly_yes_price: Some(0.25),
                poly_no_price: Some(0.75),
                cex_price: Some(2150.0),
                fair_value: Some(0.08),
                has_position: false,
                minutes_to_expiry: Some(100.0),
                active_token_side: Some("YES".to_owned()),
                poly_updated_at_ms: Some(3),
                cex_updated_at_ms: Some(4),
                poly_quote_age_ms: Some(10),
                cex_quote_age_ms: Some(20),
                cross_leg_skew_ms: Some(10),
                evaluable_status: EvaluableStatus::Evaluable,
                async_classification: AsyncClassification::BalancedFresh,
            },
        ];
        state.recent_events = vec![
            MonitorEvent {
                timestamp_ms: 1,
                kind: EventKind::Trade,
                market: Some("sol-dip-80-mar".to_owned()),
                correlation_id: None,
                summary: "sol event".to_owned(),
                details: None,
            },
            MonitorEvent {
                timestamp_ms: 2,
                kind: EventKind::Trade,
                market: Some("eth-reach-2400-mar".to_owned()),
                correlation_id: None,
                summary: "eth event".to_owned(),
                details: None,
            },
        ];
        state.recent_trades = vec![
            TradeView {
                market: "sol-dip-80-mar".to_owned(),
                opened_at_ms: 1,
                closed_at_ms: 2,
                direction: "多".to_owned(),
                token_side: "Yes".to_owned(),
                entry_price: Some(0.23),
                exit_price: Some(0.24),
                realized_pnl_usd: 1.0,
                correlation_id: None,
            },
            TradeView {
                market: "eth-reach-2400-mar".to_owned(),
                opened_at_ms: 3,
                closed_at_ms: 4,
                direction: "空".to_owned(),
                token_side: "No".to_owned(),
                entry_price: Some(0.75),
                exit_price: Some(0.74),
                realized_pnl_usd: 1.0,
                correlation_id: None,
            },
        ];
        state.recent_commands = vec![
            CommandView {
                command_id: "close-sol".to_owned(),
                kind: CommandKind::ClosePosition {
                    market: "sol-dip-80-mar".to_owned(),
                },
                status: CommandStatus::Success,
                created_at_ms: 1,
                updated_at_ms: 1,
                finished: true,
                timed_out: false,
                cancellable: false,
                message: None,
                error_code: None,
            },
            CommandView {
                command_id: "close-eth".to_owned(),
                kind: CommandKind::ClosePosition {
                    market: "eth-reach-2400-mar".to_owned(),
                },
                status: CommandStatus::Success,
                created_at_ms: 2,
                updated_at_ms: 2,
                finished: true,
                timed_out: false,
                cancellable: false,
                message: None,
                error_code: None,
            },
        ];

        let filtered = state.filter_market("sol-dip-80-mar");
        assert_eq!(filtered.positions.len(), 1);
        assert_eq!(filtered.positions[0].market, "sol-dip-80-mar");
        assert_eq!(filtered.markets.len(), 1);
        assert_eq!(filtered.markets[0].symbol, "sol-dip-80-mar");
        assert_eq!(filtered.recent_events.len(), 1);
        assert_eq!(
            filtered.recent_events[0].market.as_deref(),
            Some("sol-dip-80-mar")
        );
        assert_eq!(filtered.recent_trades.len(), 1);
        assert_eq!(filtered.recent_trades[0].market, "sol-dip-80-mar");
        assert_eq!(filtered.recent_commands.len(), 1);
        match &filtered.recent_commands[0].kind {
            CommandKind::ClosePosition { market } => assert_eq!(market, "sol-dip-80-mar"),
            kind => panic!("unexpected command kind: {kind:?}"),
        }
    }

    #[test]
    fn monitor_event_matches_market_from_context_fields() {
        let event = MonitorEvent {
            timestamp_ms: 1,
            kind: EventKind::Risk,
            market: None,
            correlation_id: Some("corr-sol-dip-80-mar-1".to_owned()),
            summary: "sol-dip-80-mar 风控提示".to_owned(),
            details: Some(HashMap::from([(
                "symbol".to_owned(),
                "sol-dip-80-mar".to_owned(),
            )])),
        };

        assert!(event.matches_market("sol-dip-80-mar"));
        assert!(!event.matches_market("eth-reach-2400-mar"));
    }
}
