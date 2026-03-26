//! Monitor 状态管理
//!
//! 聚合来自 Socket 的状态数据，供 TUI 渲染使用。

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use polyalpha_core::{
    CommandStatus, CommandView, EventKind, MarketView, MonitorEvent, MonitorState, PositionView,
    TradeView,
};

/// 可调参数
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConfigField {
    EntryZ,
    ExitZ,
    PositionNotionalUsd,
    RollingWindowSecs,
}

impl ConfigField {
    pub fn title(self) -> &'static str {
        match self {
            Self::EntryZ => "调整入场 Z 值",
            Self::ExitZ => "调整出场 Z 值",
            Self::PositionNotionalUsd => "调整单次仓位",
            Self::RollingWindowSecs => "调整滚动窗口",
        }
    }

    pub fn step(self) -> f64 {
        match self {
            Self::EntryZ | Self::ExitZ => 0.1,
            Self::PositionNotionalUsd => 500.0,
            Self::RollingWindowSecs => 60.0,
        }
    }

    pub fn min(self) -> f64 {
        match self {
            Self::EntryZ | Self::ExitZ => 0.05,
            Self::PositionNotionalUsd => 100.0,
            Self::RollingWindowSecs => 120.0,
        }
    }

    pub fn format_value(self, value: f64) -> String {
        match self {
            Self::EntryZ | Self::ExitZ => format!("{value:.2}"),
            Self::PositionNotionalUsd => format!("${value:.0}"),
            Self::RollingWindowSecs => {
                let secs = value.round().max(self.min()) as u64;
                if secs % 60 == 0 {
                    format!("{}s ({}m)", secs, secs / 60)
                } else {
                    format!("{secs}s")
                }
            }
        }
    }

    pub fn clamp(self, value: f64) -> f64 {
        value.max(self.min())
    }
}

/// 弹窗类型
#[derive(Clone, Debug, PartialEq)]
pub enum Dialog {
    ClosePositionConfirm {
        market: String,
    },
    CloseAllPositionsConfirm,
    EmergencyStopConfirm,
    ClearEmergencyConfirm,
    CancelCommandConfirm {
        command_id: String,
        summary: String,
    },
    AdjustConfig {
        field: ConfigField,
        current: f64,
        draft: f64,
    },
}

/// 页面
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Page {
    #[default]
    Overview,
    Positions,
    Equity,
    Log,
}

/// 日志页激活窗格
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogPane {
    #[default]
    Events,
    Commands,
}

/// 日志事件筛选
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogEventFilter {
    #[default]
    All,
    Signal,
    Gate,
    Trade,
    TradeClosed,
    Skip,
    Risk,
    Rejected,
    System,
    Error,
}

impl LogEventFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "全部事件",
            Self::Signal => "信号",
            Self::Gate => "闸门",
            Self::Trade => "成交",
            Self::TradeClosed => "平仓",
            Self::Skip => "跳过",
            Self::Risk => "风控",
            Self::Rejected => "拒绝",
            Self::System => "系统",
            Self::Error => "错误",
        }
    }

    fn matches(self, event: &MonitorEvent) -> bool {
        match self {
            Self::All => true,
            Self::Signal => event.kind == EventKind::Signal,
            Self::Gate => event.kind == EventKind::Gate,
            Self::Trade => event.kind == EventKind::Trade,
            Self::TradeClosed => event.kind == EventKind::TradeClosed,
            Self::Skip => event.kind == EventKind::Skip,
            Self::Risk => event.kind == EventKind::Risk,
            Self::Rejected => is_rejection_event(event),
            Self::System => event.kind == EventKind::System,
            Self::Error => event.kind == EventKind::Error,
        }
    }

    fn cycle(self, delta: isize) -> Self {
        const VALUES: [LogEventFilter; 10] = [
            LogEventFilter::All,
            LogEventFilter::Signal,
            LogEventFilter::Gate,
            LogEventFilter::Trade,
            LogEventFilter::TradeClosed,
            LogEventFilter::Skip,
            LogEventFilter::Risk,
            LogEventFilter::Rejected,
            LogEventFilter::System,
            LogEventFilter::Error,
        ];
        cycle_enum(self, &VALUES, delta)
    }
}

/// 命令筛选
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogCommandFilter {
    #[default]
    All,
    Pending,
    Running,
    Success,
    Failed,
    PartialSuccess,
}

impl LogCommandFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "全部命令",
            Self::Pending => "待处理",
            Self::Running => "执行中",
            Self::Success => "成功",
            Self::Failed => "失败",
            Self::PartialSuccess => "部分成功",
        }
    }

    fn matches(self, command: &CommandView) -> bool {
        match self {
            Self::All => true,
            Self::Pending => command.status == CommandStatus::Pending,
            Self::Running => command.status == CommandStatus::Running,
            Self::Success => command.status == CommandStatus::Success,
            Self::Failed => command.status == CommandStatus::Failed,
            Self::PartialSuccess => command.status == CommandStatus::PartialSuccess,
        }
    }

    fn cycle(self, delta: isize) -> Self {
        const VALUES: [LogCommandFilter; 6] = [
            LogCommandFilter::All,
            LogCommandFilter::Pending,
            LogCommandFilter::Running,
            LogCommandFilter::Success,
            LogCommandFilter::Failed,
            LogCommandFilter::PartialSuccess,
        ];
        cycle_enum(self, &VALUES, delta)
    }
}

/// 搜索目标
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogSearchTarget {
    Events,
    Commands,
}

impl LogSearchTarget {
    pub fn label(self) -> &'static str {
        match self {
            Self::Events => "事件",
            Self::Commands => "命令",
        }
    }
}

/// 净值页时间范围
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EquityRange {
    Last24Hours,
    Last3Days,
    Last7Days,
    #[default]
    All,
}

impl EquityRange {
    pub fn label(self) -> &'static str {
        match self {
            Self::Last24Hours => "24小时",
            Self::Last3Days => "3天",
            Self::Last7Days => "7天",
            Self::All => "全部",
        }
    }

    fn cutoff_ms(self, reference_now_ms: u64) -> Option<u64> {
        let delta_ms = match self {
            Self::Last24Hours => 24 * 60 * 60 * 1000,
            Self::Last3Days => 3 * 24 * 60 * 60 * 1000,
            Self::Last7Days => 7 * 24 * 60 * 60 * 1000,
            Self::All => return None,
        };
        Some(reference_now_ms.saturating_sub(delta_ms))
    }

    fn cycle(self, delta: isize) -> Self {
        const VALUES: [EquityRange; 4] = [
            EquityRange::Last24Hours,
            EquityRange::Last3Days,
            EquityRange::Last7Days,
            EquityRange::All,
        ];
        cycle_enum(self, &VALUES, delta)
    }
}

/// 顶部状态消息等级
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BannerLevel {
    Info,
    Success,
    Warning,
    Error,
}

/// 顶部状态消息
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BannerMessage {
    pub text: String,
    pub level: BannerLevel,
}

/// TUI 状态
#[derive(Clone, Debug)]
pub struct TuiState {
    pub monitor: MonitorState,
    pub start_time: std::time::Instant,
    pub page: Page,
    pub selected_market: usize,
    pub selected_market_symbol: Option<String>,
    pub selected_position: usize,
    pub selected_trade: usize,
    pub selected_event: usize,
    pub selected_command: usize,
    pub active_log_pane: LogPane,
    pub dialog: Option<Dialog>,
    pub show_help: bool,
    pub banner: Option<BannerMessage>,
    pub pending_ack_count: usize,
    event_filter: LogEventFilter,
    command_filter: LogCommandFilter,
    event_search_query: String,
    command_search_query: String,
    log_search_target: Option<LogSearchTarget>,
    read_event_keys: HashSet<String>,
    read_command_ids: HashSet<String>,
    cleared_event_keys: HashSet<String>,
    cleared_command_ids: HashSet<String>,
    equity_range: EquityRange,
    show_trade_detail: bool,
}

impl Default for TuiState {
    fn default() -> Self {
        Self {
            monitor: MonitorState::default(),
            start_time: std::time::Instant::now(),
            page: Page::default(),
            selected_market: 0,
            selected_market_symbol: None,
            selected_position: 0,
            selected_trade: 0,
            selected_event: 0,
            selected_command: 0,
            active_log_pane: LogPane::default(),
            dialog: None,
            show_help: false,
            banner: None,
            pending_ack_count: 0,
            event_filter: LogEventFilter::default(),
            command_filter: LogCommandFilter::default(),
            event_search_query: String::new(),
            command_search_query: String::new(),
            log_search_target: None,
            read_event_keys: HashSet::new(),
            read_command_ids: HashSet::new(),
            cleared_event_keys: HashSet::new(),
            cleared_command_ids: HashSet::new(),
            equity_range: EquityRange::default(),
            show_trade_detail: false,
        }
    }
}

impl TuiState {
    pub fn update(&mut self, state: MonitorState) {
        self.monitor = state;
        self.prune_local_log_state();
        self.clamp_selection();
        self.sync_selected_market_index();
    }

    pub fn uptime_secs(&self) -> u64 {
        if self.monitor.uptime_secs > 0 {
            self.monitor.uptime_secs
        } else {
            self.start_time.elapsed().as_secs()
        }
    }

    pub fn state_age_ms(&self) -> Option<u64> {
        if self.monitor.timestamp_ms == 0 {
            None
        } else {
            Some(now_millis().saturating_sub(self.monitor.timestamp_ms))
        }
    }

    pub fn data_age_ms(&self, updated_at_ms: Option<u64>) -> Option<u64> {
        updated_at_ms.map(|timestamp_ms| now_millis().saturating_sub(timestamp_ms))
    }

    pub fn state_is_stale(&self) -> bool {
        self.state_age_ms().is_some_and(|age_ms| age_ms > 5_000)
    }

    pub fn switch_page(&mut self, page: Page) {
        self.page = page;
        if page != Page::Log {
            self.log_search_target = None;
        }
        self.clamp_selection();
    }

    pub fn select_next_market(&mut self) {
        bump_index(&mut self.selected_market, self.monitor.markets.len(), 1);
        self.sync_selected_market_symbol();
    }

    pub fn select_prev_market(&mut self) {
        bump_index(&mut self.selected_market, self.monitor.markets.len(), -1);
        self.sync_selected_market_symbol();
    }

    pub fn select_next_position(&mut self) {
        bump_index(&mut self.selected_position, self.monitor.positions.len(), 1);
    }

    pub fn select_prev_position(&mut self) {
        bump_index(
            &mut self.selected_position,
            self.monitor.positions.len(),
            -1,
        );
    }

    pub fn select_next_trade(&mut self) {
        let visible_len = self.visible_trades().len();
        bump_index(&mut self.selected_trade, visible_len, 1);
    }

    pub fn select_prev_trade(&mut self) {
        let visible_len = self.visible_trades().len();
        bump_index(&mut self.selected_trade, visible_len, -1);
    }

    pub fn select_next_event(&mut self) {
        let visible_len = self.visible_events().len();
        bump_index(&mut self.selected_event, visible_len, 1);
    }

    pub fn select_prev_event(&mut self) {
        let visible_len = self.visible_events().len();
        bump_index(&mut self.selected_event, visible_len, -1);
    }

    pub fn select_next_command(&mut self) {
        let visible_len = self.visible_commands().len();
        bump_index(&mut self.selected_command, visible_len, 1);
    }

    pub fn select_prev_command(&mut self) {
        let visible_len = self.visible_commands().len();
        bump_index(&mut self.selected_command, visible_len, -1);
    }

    pub fn toggle_log_pane(&mut self) {
        self.active_log_pane = match self.active_log_pane {
            LogPane::Events => LogPane::Commands,
            LogPane::Commands => LogPane::Events,
        };
        self.clamp_selection();
    }

    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    pub fn show_dialog(&mut self, dialog: Dialog) {
        self.dialog = Some(dialog);
    }

    pub fn close_dialog(&mut self) {
        self.dialog = None;
    }

    pub fn has_dialog(&self) -> bool {
        self.dialog.is_some()
    }

    pub fn open_adjust_config(&mut self, field: ConfigField) {
        let current = match field {
            ConfigField::EntryZ => self.monitor.config.entry_z,
            ConfigField::ExitZ => self.monitor.config.exit_z,
            ConfigField::PositionNotionalUsd => self.monitor.config.position_notional_usd,
            ConfigField::RollingWindowSecs => self.monitor.config.rolling_window as f64,
        };
        self.dialog = Some(Dialog::AdjustConfig {
            field,
            current,
            draft: current,
        });
    }

    pub fn nudge_dialog(&mut self, delta: isize) {
        let Some(Dialog::AdjustConfig { field, draft, .. }) = &mut self.dialog else {
            return;
        };

        let next = field.clamp(*draft + field.step() * delta as f64);
        *draft = match field {
            ConfigField::EntryZ => next,
            ConfigField::ExitZ => next.min(self.monitor.config.entry_z.max(field.min())),
            ConfigField::PositionNotionalUsd => next,
            ConfigField::RollingWindowSecs => next.round(),
        };
    }

    pub fn set_banner(&mut self, text: impl Into<String>, level: BannerLevel) {
        self.banner = Some(BannerMessage {
            text: text.into(),
            level,
        });
    }

    pub fn set_pending_ack_count(&mut self, count: usize) {
        self.pending_ack_count = count;
    }

    pub fn pending_command_count(&self) -> usize {
        self.pending_ack_count.max(
            self.monitor
                .recent_commands
                .iter()
                .filter(|command| !command.finished)
                .count(),
        )
    }

    pub fn selected_position(&self) -> Option<&PositionView> {
        self.monitor.positions.get(self.selected_position)
    }

    pub fn selected_trade(&self) -> Option<&TradeView> {
        self.visible_trades().get(self.selected_trade).copied()
    }

    pub fn selected_market(&self) -> Option<&MarketView> {
        sorted_markets(&self.monitor.markets)
            .get(self.selected_market)
            .copied()
    }

    pub fn selected_command(&self) -> Option<&CommandView> {
        self.visible_commands().get(self.selected_command).copied()
    }

    pub fn selected_event(&self) -> Option<&MonitorEvent> {
        self.visible_events().get(self.selected_event).copied()
    }

    pub fn visible_trades(&self) -> Vec<&TradeView> {
        let cutoff_ms = self.equity_range.cutoff_ms(self.reference_now_ms());
        self.monitor
            .recent_trades
            .iter()
            .filter(|trade| cutoff_ms.is_none_or(|cutoff| trade.closed_at_ms >= cutoff))
            .collect()
    }

    pub fn visible_events(&self) -> Vec<&MonitorEvent> {
        self.monitor
            .recent_events
            .iter()
            .filter(|event| self.event_filter.matches(event))
            .filter(|event| matches_event_query(event, &self.event_search_query))
            .filter(|event| !self.cleared_event_keys.contains(&event_key(event)))
            .collect()
    }

    pub fn visible_commands(&self) -> Vec<&CommandView> {
        self.monitor
            .recent_commands
            .iter()
            .filter(|command| self.command_filter.matches(command))
            .filter(|command| matches_command_query(command, &self.command_search_query))
            .filter(|command| !self.cleared_command_ids.contains(&command.command_id))
            .collect()
    }

    pub fn equity_range(&self) -> EquityRange {
        self.equity_range
    }

    pub fn cycle_equity_range(&mut self, delta: isize) {
        self.equity_range = self.equity_range.cycle(delta);
        self.clamp_selection();
    }

    pub fn show_trade_detail(&self) -> bool {
        self.show_trade_detail
    }

    pub fn toggle_trade_detail(&mut self) {
        self.show_trade_detail = !self.show_trade_detail;
    }

    pub fn event_filter(&self) -> LogEventFilter {
        self.event_filter
    }

    pub fn command_filter(&self) -> LogCommandFilter {
        self.command_filter
    }

    pub fn cycle_active_log_filter(&mut self, delta: isize) {
        match self.active_log_pane {
            LogPane::Events => self.event_filter = self.event_filter.cycle(delta),
            LogPane::Commands => self.command_filter = self.command_filter.cycle(delta),
        }
        self.clamp_selection();
    }

    pub fn event_search_query(&self) -> &str {
        &self.event_search_query
    }

    pub fn command_search_query(&self) -> &str {
        &self.command_search_query
    }

    pub fn begin_log_search(&mut self) {
        self.log_search_target = Some(match self.active_log_pane {
            LogPane::Events => LogSearchTarget::Events,
            LogPane::Commands => LogSearchTarget::Commands,
        });
    }

    pub fn finish_log_search(&mut self) {
        self.log_search_target = None;
        self.clamp_selection();
    }

    pub fn cancel_log_search(&mut self) {
        self.log_search_target = None;
    }

    pub fn is_editing_log_search(&self) -> bool {
        self.log_search_target.is_some()
    }

    pub fn log_search_target(&self) -> Option<LogSearchTarget> {
        self.log_search_target
    }

    pub fn push_log_search_char(&mut self, ch: char) {
        if ch.is_control() {
            return;
        }
        match self.log_search_target {
            Some(LogSearchTarget::Events) => self.event_search_query.push(ch),
            Some(LogSearchTarget::Commands) => self.command_search_query.push(ch),
            None => return,
        }
        self.clamp_selection();
    }

    pub fn pop_log_search_char(&mut self) {
        match self.log_search_target {
            Some(LogSearchTarget::Events) => {
                self.event_search_query.pop();
            }
            Some(LogSearchTarget::Commands) => {
                self.command_search_query.pop();
            }
            None => return,
        }
        self.clamp_selection();
    }

    pub fn clear_active_log_search(&mut self) {
        match self.active_log_pane {
            LogPane::Events => self.event_search_query.clear(),
            LogPane::Commands => self.command_search_query.clear(),
        }
        self.clamp_selection();
    }

    pub fn visible_event_unread_count(&self) -> usize {
        self.visible_events()
            .into_iter()
            .filter(|event| !self.read_event_keys.contains(&event_key(event)))
            .count()
    }

    pub fn visible_command_unread_count(&self) -> usize {
        self.visible_commands()
            .into_iter()
            .filter(|command| !self.read_command_ids.contains(&command.command_id))
            .count()
    }

    pub fn cleared_event_count(&self) -> usize {
        self.monitor
            .recent_events
            .iter()
            .filter(|event| self.cleared_event_keys.contains(&event_key(event)))
            .count()
    }

    pub fn cleared_command_count(&self) -> usize {
        self.monitor
            .recent_commands
            .iter()
            .filter(|command| self.cleared_command_ids.contains(&command.command_id))
            .count()
    }

    pub fn mark_active_log_pane_read(&mut self) -> usize {
        match self.active_log_pane {
            LogPane::Events => {
                let keys = self
                    .visible_events()
                    .into_iter()
                    .map(event_key)
                    .collect::<Vec<_>>();
                let mut marked = 0usize;
                for key in keys {
                    if self.read_event_keys.insert(key) {
                        marked += 1;
                    }
                }
                marked
            }
            LogPane::Commands => {
                let ids = self
                    .visible_commands()
                    .into_iter()
                    .map(|command| command.command_id.clone())
                    .collect::<Vec<_>>();
                let mut marked = 0usize;
                for id in ids {
                    if self.read_command_ids.insert(id) {
                        marked += 1;
                    }
                }
                marked
            }
        }
    }

    pub fn clear_active_log_pane_read(&mut self) -> usize {
        match self.active_log_pane {
            LogPane::Events => {
                let keys = self
                    .visible_events()
                    .into_iter()
                    .map(event_key)
                    .collect::<Vec<_>>();
                let mut cleared = 0usize;
                for key in keys {
                    if self.read_event_keys.contains(&key) && self.cleared_event_keys.insert(key) {
                        cleared += 1;
                    }
                }
                self.clamp_selection();
                cleared
            }
            LogPane::Commands => {
                let ids = self
                    .visible_commands()
                    .into_iter()
                    .map(|command| command.command_id.clone())
                    .collect::<Vec<_>>();
                let mut cleared = 0usize;
                for id in ids {
                    if self.read_command_ids.contains(&id) && self.cleared_command_ids.insert(id) {
                        cleared += 1;
                    }
                }
                self.clamp_selection();
                cleared
            }
        }
    }

    pub fn is_event_read(&self, event: &MonitorEvent) -> bool {
        self.read_event_keys.contains(&event_key(event))
    }

    pub fn is_command_read(&self, command: &CommandView) -> bool {
        self.read_command_ids.contains(&command.command_id)
    }

    fn clamp_selection(&mut self) {
        let visible_trade_len = self.visible_trades().len();
        let visible_event_len = self.visible_events().len();
        let visible_command_len = self.visible_commands().len();
        clamp_index(&mut self.selected_market, self.monitor.markets.len());
        clamp_index(&mut self.selected_position, self.monitor.positions.len());
        clamp_index(&mut self.selected_trade, visible_trade_len);
        clamp_index(&mut self.selected_event, visible_event_len);
        clamp_index(&mut self.selected_command, visible_command_len);
    }

    fn sync_selected_market_symbol(&mut self) {
        self.selected_market_symbol = self.selected_market().map(|market| market.symbol.clone());
    }

    fn sync_selected_market_index(&mut self) {
        let sorted = sorted_markets(&self.monitor.markets);
        if sorted.is_empty() {
            self.selected_market = 0;
            self.selected_market_symbol = None;
            return;
        }

        if let Some(symbol) = self.selected_market_symbol.as_deref() {
            if let Some(idx) = sorted.iter().position(|market| market.symbol == symbol) {
                self.selected_market = idx;
            }
        }

        clamp_index(&mut self.selected_market, sorted.len());
        self.selected_market_symbol = sorted
            .get(self.selected_market)
            .map(|market| market.symbol.clone());
    }

    fn prune_local_log_state(&mut self) {
        let event_keys = self
            .monitor
            .recent_events
            .iter()
            .map(event_key)
            .collect::<HashSet<_>>();
        self.read_event_keys.retain(|key| event_keys.contains(key));
        self.cleared_event_keys
            .retain(|key| event_keys.contains(key));

        let command_ids = self
            .monitor
            .recent_commands
            .iter()
            .map(|command| command.command_id.clone())
            .collect::<HashSet<_>>();
        self.read_command_ids
            .retain(|command_id| command_ids.contains(command_id));
        self.cleared_command_ids
            .retain(|command_id| command_ids.contains(command_id));
    }

    fn reference_now_ms(&self) -> u64 {
        if self.monitor.timestamp_ms > 0 {
            self.monitor.timestamp_ms
        } else {
            now_millis()
        }
    }
}

fn clamp_index(index: &mut usize, len: usize) {
    if len == 0 {
        *index = 0;
    } else if *index >= len {
        *index = len - 1;
    }
}

fn bump_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
        return;
    }

    let next = (*index as isize + delta).clamp(0, len.saturating_sub(1) as isize);
    *index = next as usize;
}

fn cycle_enum<T: Copy + PartialEq>(current: T, values: &[T], delta: isize) -> T {
    let Some(index) = values.iter().position(|value| *value == current) else {
        return values[0];
    };
    let len = values.len() as isize;
    let next = (index as isize + delta).rem_euclid(len) as usize;
    values[next]
}

fn event_key(event: &MonitorEvent) -> String {
    format!(
        "{}|{:?}|{}|{}|{}",
        event.timestamp_ms,
        event.kind,
        event.market.as_deref().unwrap_or_default(),
        event.correlation_id.as_deref().unwrap_or_default(),
        event.summary
    )
}

fn matches_event_query(event: &MonitorEvent, query: &str) -> bool {
    let needle = normalize_query(query);
    if needle.is_empty() {
        return true;
    }

    let mut haystacks = vec![
        event.summary.as_str(),
        event.market.as_deref().unwrap_or_default(),
        event.correlation_id.as_deref().unwrap_or_default(),
    ];
    let detail_values = event
        .details
        .iter()
        .flat_map(|details| details.iter())
        .flat_map(|(key, value)| [key.as_str(), value.as_str()])
        .collect::<Vec<_>>();
    haystacks.extend(detail_values);

    haystacks
        .into_iter()
        .map(normalize_query)
        .any(|haystack| haystack.contains(&needle))
}

fn is_rejection_event(event: &MonitorEvent) -> bool {
    if normalize_query(&event.summary).contains("拒绝") {
        return true;
    }

    event
        .details
        .iter()
        .flat_map(|details| details.iter())
        .any(|(key, value)| {
            let key = normalize_query(key);
            let value = normalize_query(value);
            key.contains("拒绝") || value.contains("拒绝") || value == "rejected"
        })
}

fn matches_command_query(command: &CommandView, query: &str) -> bool {
    let needle = normalize_query(query);
    if needle.is_empty() {
        return true;
    }

    [
        command.command_id.as_str(),
        command.message.as_deref().unwrap_or_default(),
    ]
    .into_iter()
    .map(normalize_query)
    .chain([format!("{:?}", command.kind).to_lowercase()])
    .any(|haystack| haystack.contains(&needle))
}

fn normalize_query(value: &str) -> String {
    value.trim().to_lowercase()
}

fn sorted_markets(markets: &[MarketView]) -> Vec<&MarketView> {
    let mut items = markets.iter().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        market_rank(right)
            .partial_cmp(&market_rank(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    items
}

fn market_rank(market: &MarketView) -> f64 {
    market
        .z_score
        .map(|value| value.abs())
        .or_else(|| market.basis_pct.map(|value| value.abs()))
        .unwrap_or(-1.0)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use polyalpha_core::{CommandKind, CommandView, MonitorEvent, MonitorState};

    #[test]
    fn log_filters_search_and_clear_read_work_together() {
        let mut state = TuiState::default();
        state.page = Page::Log;
        state.active_log_pane = LogPane::Events;
        state.update(sample_monitor_state());

        assert_eq!(state.visible_events().len(), 3);

        state.cycle_active_log_filter(1);
        assert_eq!(state.event_filter(), LogEventFilter::Signal);
        assert_eq!(state.visible_events().len(), 1);
        assert_eq!(state.visible_events()[0].summary, "btc 信号已触发");

        state.begin_log_search();
        for ch in "eth".chars() {
            state.push_log_search_char(ch);
        }
        state.finish_log_search();
        assert!(state.visible_events().is_empty());

        state.clear_active_log_search();
        assert_eq!(state.visible_events().len(), 1);

        state.cycle_active_log_filter(-1);
        assert_eq!(state.event_filter(), LogEventFilter::All);
        assert_eq!(state.visible_events().len(), 3);

        for _ in 0..7 {
            state.cycle_active_log_filter(1);
        }
        assert_eq!(state.event_filter(), LogEventFilter::Rejected);
        assert_eq!(state.visible_events().len(), 1);
        assert_eq!(state.visible_events()[0].summary, "eth 风控拒绝");

        for _ in 0..3 {
            state.cycle_active_log_filter(1);
        }
        assert_eq!(state.event_filter(), LogEventFilter::All);

        assert_eq!(state.mark_active_log_pane_read(), 3);
        assert_eq!(state.visible_event_unread_count(), 0);
        assert_eq!(state.clear_active_log_pane_read(), 3);
        assert!(state.visible_events().is_empty());
        assert_eq!(state.cleared_event_count(), 3);
    }

    #[test]
    fn command_filter_search_and_read_state_apply_to_visible_list() {
        let mut state = TuiState::default();
        state.page = Page::Log;
        state.active_log_pane = LogPane::Commands;
        state.update(sample_monitor_state());

        assert_eq!(state.visible_commands().len(), 2);
        state.cycle_active_log_filter(-1);
        assert_eq!(state.command_filter(), LogCommandFilter::PartialSuccess);
        assert!(state.visible_commands().is_empty());

        state.cycle_active_log_filter(2);
        assert_eq!(state.command_filter(), LogCommandFilter::Pending);
        assert_eq!(state.visible_commands().len(), 1);

        state.begin_log_search();
        for ch in "pause".chars() {
            state.push_log_search_char(ch);
        }
        state.finish_log_search();
        assert_eq!(state.visible_commands().len(), 1);
        assert_eq!(state.visible_commands()[0].command_id, "cmd-1");

        assert_eq!(state.mark_active_log_pane_read(), 1);
        assert_eq!(state.clear_active_log_pane_read(), 1);
        assert!(state.visible_commands().is_empty());
    }

    fn sample_monitor_state() -> MonitorState {
        MonitorState {
            timestamp_ms: 1_800_000_000_000,
            recent_events: vec![
                MonitorEvent {
                    timestamp_ms: 1,
                    kind: EventKind::Signal,
                    market: Some("btc".to_owned()),
                    correlation_id: Some("sig-1".to_owned()),
                    summary: "btc 信号已触发".to_owned(),
                    details: None,
                },
                MonitorEvent {
                    timestamp_ms: 2,
                    kind: EventKind::Risk,
                    market: Some("eth".to_owned()),
                    correlation_id: None,
                    summary: "eth 风控拒绝".to_owned(),
                    details: Some(HashMap::from([("原因".to_owned(), "报价过期".to_owned())])),
                },
                MonitorEvent {
                    timestamp_ms: 3,
                    kind: EventKind::System,
                    market: None,
                    correlation_id: None,
                    summary: "系统恢复".to_owned(),
                    details: None,
                },
            ],
            recent_commands: vec![
                CommandView {
                    command_id: "cmd-1".to_owned(),
                    kind: CommandKind::Pause,
                    status: CommandStatus::Pending,
                    created_at_ms: 10,
                    updated_at_ms: 11,
                    finished: false,
                    timed_out: false,
                    cancellable: true,
                    message: Some("pause queued".to_owned()),
                    error_code: None,
                },
                CommandView {
                    command_id: "cmd-2".to_owned(),
                    kind: CommandKind::Resume,
                    status: CommandStatus::Success,
                    created_at_ms: 12,
                    updated_at_ms: 13,
                    finished: true,
                    timed_out: false,
                    cancellable: false,
                    message: Some("resume ok".to_owned()),
                    error_code: None,
                },
            ],
            ..MonitorState::default()
        }
    }
}
