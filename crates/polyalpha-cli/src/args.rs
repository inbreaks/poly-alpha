use clap::{Parser, Subcommand, ValueEnum};

fn default_monitor_socket_path() -> String {
    std::env::var("POLYALPHA__GENERAL__MONITOR_SOCKET_PATH")
        .unwrap_or_else(|_| "/tmp/polyalpha.sock".to_owned())
}

#[derive(Parser, Debug)]
#[command(name = "polyalpha-cli", about = "PolyAlpha MVP CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
    CheckConfig {
        #[arg(long, default_value = "default")]
        env: String,
    },
    Demo {
        #[arg(long, default_value = "default")]
        env: String,
    },
    Live {
        #[command(subcommand)]
        command: LiveCommand,
    },
    LiveDataCheck {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
    },
    LiveExecPreview {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, value_enum)]
        exchange: PreviewExchange,
        #[arg(long, value_enum)]
        side: PreviewSide,
        #[arg(long, value_enum, default_value_t = PreviewOrderType::Market)]
        order_type: PreviewOrderType,
        #[arg(long)]
        qty: String,
        #[arg(long)]
        price: Option<String>,
        #[arg(long, default_value_t = false)]
        reduce_only: bool,
    },
    Markets {
        #[command(subcommand)]
        command: MarketCommand,
    },
    Sim {
        #[command(subcommand)]
        command: SimCommand,
    },
    Paper {
        #[command(subcommand)]
        command: PaperCommand,
    },
    Backtest {
        #[command(subcommand)]
        command: BacktestCommand,
    },
    /// 启动监控 TUI
    Monitor {
        #[arg(long, default_value_t = default_monitor_socket_path())]
        socket: String,
        #[arg(
            long,
            default_value_t = false,
            help = "JSON 流输出模式",
            conflicts_with = "snapshot"
        )]
        json: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "一次性状态快照",
            conflicts_with = "json"
        )]
        snapshot: bool,
        #[arg(long, default_value_t = 1000, help = "JSON 流最小输出间隔（毫秒）")]
        interval_ms: u64,
        #[arg(long, default_value_t = 5000, help = "快照等待超时（毫秒）")]
        timeout_ms: u64,
        #[arg(long, help = "仅监控指定市场")]
        market: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "启用鼠标捕获（会阻止终端拖拽选中文本）"
        )]
        mouse_capture: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "禁用 alternate screen，把界面保留在终端滚动历史里"
        )]
        no_alternate_screen: bool,
    },
    /// JSON 输出模式 (agent 友好)
    MonitorJson {
        #[arg(long, default_value_t = default_monitor_socket_path())]
        socket: String,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
        #[arg(long, help = "仅监控指定市场")]
        market: Option<String>,
    },
    /// 一次性状态快照
    MonitorSnapshot {
        #[arg(long, default_value_t = default_monitor_socket_path())]
        socket: String,
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
        #[arg(long, help = "仅监控指定市场")]
        market: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum AuditCommand {
    Events {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        gate: Option<String>,
        #[arg(long)]
        result: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value_t = 30)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
    SessionSummary {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
    Market {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
    PositionExplain {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
    /// 复盘已平仓交易时间线
    TradeTimeline {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, help = "指定审计会话；默认读取该环境最近一轮")]
        session_id: Option<String>,
        #[arg(long, help = "仅复盘指定市场的已平仓交易")]
        symbol: Option<String>,
        #[arg(long, conflicts_with = "trade", help = "直接定位某一笔平仓链")]
        correlation_id: Option<String>,
        #[arg(
            long,
            conflicts_with = "correlation_id",
            help = "按列表编号查看单笔详情（从 1 开始）"
        )]
        trade: Option<usize>,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
    /// 复盘单笔开仓决策链
    OpenDecision {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, help = "指定审计会话；默认读取该环境最近一轮")]
        session_id: Option<String>,
        #[arg(long, help = "仅查看指定市场的开仓决策")]
        symbol: Option<String>,
        #[arg(long, conflicts_with = "entry", help = "直接定位某一笔开仓链")]
        correlation_id: Option<String>,
        #[arg(
            long,
            conflicts_with = "correlation_id",
            help = "按列表编号查看单笔详情（从 1 开始）"
        )]
        entry: Option<usize>,
        #[arg(long, value_enum, default_value_t = AuditOutputFormat::Table)]
        format: AuditOutputFormat,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum AuditOutputFormat {
    #[default]
    Table,
    Json,
}

#[derive(Subcommand, Debug)]
pub enum SimCommand {
    Run {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, value_enum, default_value_t = SimScenario::Basic)]
        scenario: SimScenario,
        #[arg(long, default_value_t = 300)]
        tick_interval_ms: u64,
        #[arg(long, default_value_t = 1)]
        print_every: usize,
        #[arg(long, default_value_t = 0)]
        max_ticks: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Inspect {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, value_enum, default_value_t = SimInspectFormat::Table)]
        format: SimInspectFormat,
    },
}

#[derive(Subcommand, Debug)]
pub enum LiveCommand {
    Run {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, default_value_t = 2_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 1)]
        print_every: usize,
        #[arg(long, default_value_t = 0)]
        max_ticks: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
        #[arg(long, default_value_t = true)]
        include_funding: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = 600)]
        warmup_klines: u16,
        #[arg(long, value_enum, default_value_t = LiveExecutorMode::Mock)]
        executor_mode: LiveExecutorMode,
        #[arg(long, default_value_t = false)]
        confirm_live: bool,
    },
    RunMulti {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 5_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 1)]
        print_every: usize,
        #[arg(long, default_value_t = 0)]
        max_ticks: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
        #[arg(long, default_value_t = true)]
        include_funding: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = 600)]
        warmup_klines: u16,
        #[arg(long, value_enum, default_value_t = LiveExecutorMode::Mock)]
        executor_mode: LiveExecutorMode,
        #[arg(long, default_value_t = false)]
        confirm_live: bool,
    },
    DataCheck {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
    },
    ExecPreview {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, value_enum)]
        exchange: PreviewExchange,
        #[arg(long, value_enum)]
        side: PreviewSide,
        #[arg(long, value_enum, default_value_t = PreviewOrderType::Market)]
        order_type: PreviewOrderType,
        #[arg(long)]
        qty: String,
        #[arg(long)]
        price: Option<String>,
        #[arg(long, default_value_t = false)]
        reduce_only: bool,
    },
    Inspect {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, value_enum, default_value_t = PaperInspectFormat::Table)]
        format: PaperInspectFormat,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum, Default, PartialEq, Eq)]
pub enum LiveExecutorMode {
    #[default]
    Mock,
    Live,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PreviewExchange {
    Binance,
    Okx,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PreviewSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum PreviewOrderType {
    #[default]
    Market,
    Limit,
    PostOnly,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum SimScenario {
    #[default]
    Basic,
    BasisEntry,
    PhaseCloseOnly,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum SimInspectFormat {
    #[default]
    Table,
    Json,
}

#[derive(Subcommand, Debug)]
pub enum PaperCommand {
    Run {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        market_index: usize,
        #[arg(long, default_value_t = 2_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 1)]
        print_every: usize,
        #[arg(long, default_value_t = 0)]
        max_ticks: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
        #[arg(long, default_value_t = true)]
        include_funding: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = 600)]
        warmup_klines: u16,
    },
    /// Run paper trading for ALL markets in a single process (shared risk management)
    RunMulti {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 5_000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 1)]
        print_every: usize,
        #[arg(long, default_value_t = 0)]
        max_ticks: usize,
        #[arg(long, default_value_t = 20)]
        depth: u16,
        #[arg(long, default_value_t = true)]
        include_funding: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = 600)]
        warmup_klines: u16,
    },
    Inspect {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, value_enum, default_value_t = PaperInspectFormat::Table)]
        format: PaperInspectFormat,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum PaperInspectFormat {
    #[default]
    Table,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum RustStressPreset {
    #[default]
    Baseline,
    Conservative,
    Harsh,
}

#[derive(Subcommand, Debug)]
pub enum BacktestCommand {
    PrepareDb {
        #[arg(long)]
        start_date: Option<String>,
        #[arg(long)]
        end_date: Option<String>,
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_ready.duckdb"
        )]
        output: String,
        #[arg(long, default_value = "btc")]
        asset: String,
        #[arg(long, default_value_t = 0)]
        max_events: usize,
        #[arg(long, default_value_t = 0)]
        max_contracts: usize,
        #[arg(long, default_value_t = false)]
        allow_spot_fallback: bool,
    },
    InspectDb {
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_ready.duckdb"
        )]
        db_path: String,
        #[arg(long, default_value_t = false)]
        show_failures: bool,
    },
    Run {
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_ready.duckdb"
        )]
        db_path: String,
        #[arg(long)]
        market_id: Option<String>,
        #[arg(long)]
        token_id: Option<String>,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        initial_capital: Option<f64>,
        #[arg(long)]
        rolling_window: Option<usize>,
        #[arg(long)]
        entry_z: Option<f64>,
        #[arg(long)]
        exit_z: Option<f64>,
        #[arg(long)]
        position_units: Option<f64>,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long)]
        max_capital_usage: Option<f64>,
        #[arg(long)]
        cex_hedge_ratio: Option<f64>,
        #[arg(long)]
        cex_margin_ratio: Option<f64>,
        #[arg(long)]
        fee_bps: Option<f64>,
        #[arg(long)]
        slippage_bps: Option<f64>,
        #[arg(long)]
        report_json: Option<String>,
        #[arg(long)]
        equity_csv: Option<String>,
    },
    WalkForward {
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_ready.duckdb"
        )]
        db_path: String,
        #[arg(long)]
        market_id: Option<String>,
        #[arg(long)]
        token_id: Option<String>,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        initial_capital: Option<f64>,
        #[arg(long)]
        rolling_window: Option<usize>,
        #[arg(long)]
        entry_z: Option<f64>,
        #[arg(long)]
        exit_z: Option<f64>,
        #[arg(long)]
        position_units: Option<f64>,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long)]
        max_capital_usage: Option<f64>,
        #[arg(long)]
        cex_hedge_ratio: Option<f64>,
        #[arg(long)]
        cex_margin_ratio: Option<f64>,
        #[arg(long)]
        fee_bps: Option<f64>,
        #[arg(long)]
        slippage_bps: Option<f64>,
        #[arg(long)]
        train_days: Option<usize>,
        #[arg(long)]
        test_days: Option<usize>,
        #[arg(long)]
        step_days: Option<usize>,
        #[arg(long)]
        max_slices: Option<usize>,
        #[arg(long)]
        report_json: Option<String>,
        #[arg(long)]
        equity_csv: Option<String>,
    },
    RustReplay {
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_2025_2026.duckdb"
        )]
        db_path: String,
        #[arg(long)]
        market_id: Option<String>,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, help = "默认取自 config/default.toml")]
        initial_capital: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        rolling_window: Option<usize>,
        #[arg(long, help = "默认取自 config/default.toml")]
        entry_z: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        exit_z: Option<f64>,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        max_capital_usage: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_hedge_ratio: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_margin_ratio: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        poly_fee_bps: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        poly_slippage_bps: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_fee_bps: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_slippage_bps: Option<f64>,
        #[arg(long, default_value_t = 1.0)]
        entry_fill_ratio: f64,
        #[arg(long, help = "默认取自 config/default.toml")]
        planner_depth_levels: Option<usize>,
        #[arg(
            long,
            value_parser = ["configured_band", "disabled"],
            help = "显式控制 Polymarket 价格带策略"
        )]
        band_policy: Option<String>,
        #[arg(long)]
        min_poly_price: Option<f64>,
        #[arg(long)]
        max_poly_price: Option<f64>,
        #[arg(long)]
        max_holding_bars: Option<usize>,
        #[arg(long)]
        report_json: Option<String>,
        #[arg(long)]
        anomaly_report_json: Option<String>,
        #[arg(long, default_value_t = false)]
        fail_on_anomaly: bool,
        #[arg(long)]
        equity_csv: Option<String>,
        #[arg(long)]
        trades_csv: Option<String>,
        #[arg(long)]
        snapshots_csv: Option<String>,
    },
    RustStress {
        #[arg(
            long,
            default_value = "data/btc_basis_backtest_price_only_2025_2026.duckdb"
        )]
        db_path: String,
        #[arg(long)]
        market_id: Option<String>,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, help = "默认取自 config/default.toml")]
        initial_capital: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        rolling_window: Option<usize>,
        #[arg(long, help = "默认取自 config/default.toml")]
        entry_z: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        exit_z: Option<f64>,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        max_capital_usage: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_hedge_ratio: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        cex_margin_ratio: Option<f64>,
        #[arg(long, help = "默认取自 config/default.toml")]
        planner_depth_levels: Option<usize>,
        #[arg(long, value_enum)]
        preset: Option<RustStressPreset>,
        #[arg(long)]
        report_json: Option<String>,
    },
}

#[cfg(test)]
mod live_command_tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_live_run_with_executor_mode_and_confirmation() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "live",
            "run",
            "--env",
            "prod",
            "--market-index",
            "2",
            "--executor-mode",
            "live",
            "--confirm-live",
        ])
        .expect("live run command should parse");

        match cli.command {
            Command::Live {
                command:
                    LiveCommand::Run {
                        env,
                        market_index,
                        executor_mode,
                        confirm_live,
                        ..
                    },
            } => {
                assert_eq!(env, "prod");
                assert_eq!(market_index, 2);
                assert_eq!(executor_mode, LiveExecutorMode::Live);
                assert!(confirm_live);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_live_data_check_subcommand() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "live",
            "data-check",
            "--env",
            "staging",
            "--market-index",
            "1",
            "--depth",
            "25",
        ])
        .expect("live data-check command should parse");

        match cli.command {
            Command::Live {
                command:
                    LiveCommand::DataCheck {
                        env,
                        market_index,
                        depth,
                    },
            } => {
                assert_eq!(env, "staging");
                assert_eq!(market_index, 1);
                assert_eq!(depth, 25);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_live_inspect_subcommand() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "live",
            "inspect",
            "--env",
            "prod",
            "--format",
            "json",
        ])
        .expect("live inspect command should parse");

        match cli.command {
            Command::Live {
                command: LiveCommand::Inspect { env, format },
            } => {
                assert_eq!(env, "prod");
                assert!(matches!(format, PaperInspectFormat::Json));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_backtest_rust_replay_anomaly_flags() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "backtest",
            "rust-replay",
            "--db-path",
            "data/replay.duckdb",
            "--report-json",
            "report.json",
            "--anomaly-report-json",
            "anomalies.json",
            "--fail-on-anomaly",
        ])
        .expect("backtest rust-replay command should parse");

        match cli.command {
            Command::Backtest {
                command:
                    BacktestCommand::RustReplay {
                        db_path,
                        report_json,
                        anomaly_report_json,
                        fail_on_anomaly,
                        ..
                    },
            } => {
                assert_eq!(db_path, "data/replay.duckdb");
                assert_eq!(report_json.as_deref(), Some("report.json"));
                assert_eq!(anomaly_report_json.as_deref(), Some("anomalies.json"));
                assert!(fail_on_anomaly);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_backtest_rust_replay_band_policy() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "backtest",
            "rust-replay",
            "--band-policy",
            "disabled",
        ])
        .expect("backtest rust-replay band policy should parse");

        match cli.command {
            Command::Backtest {
                command: BacktestCommand::RustReplay { band_policy, .. },
            } => assert_eq!(band_policy.as_deref(), Some("disabled")),
            other => panic!("unexpected command: {other:?}"),
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum MarketCommand {
    DiscoverBtc {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        template_market_index: Option<usize>,
        #[arg(long, default_value = "bitcoin")]
        query: String,
        #[arg(long)]
        match_text: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        pick: Option<usize>,
        #[arg(long)]
        output: Option<String>,
    },
    DiscoverEth {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        template_market_index: Option<usize>,
        #[arg(long, default_value = "ethereum")]
        query: String,
        #[arg(long)]
        match_text: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        pick: Option<usize>,
        #[arg(long)]
        output: Option<String>,
    },
    DiscoverSol {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        template_market_index: Option<usize>,
        #[arg(long, default_value = "solana")]
        query: String,
        #[arg(long)]
        match_text: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        pick: Option<usize>,
        #[arg(long)]
        output: Option<String>,
    },
    DiscoverXrp {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long)]
        template_market_index: Option<usize>,
        #[arg(long, default_value = "xrp")]
        query: String,
        #[arg(long)]
        match_text: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        pick: Option<usize>,
        #[arg(long)]
        output: Option<String>,
    },
    RefreshActive {
        #[arg(long, default_value = "multi-market-active")]
        base_env: String,
        #[arg(long, default_value = "all-markets")]
        catalog_env: String,
        #[arg(long, value_enum)]
        asset: Vec<MarketAsset>,
        #[arg(long, default_value_t = 300)]
        limit_per_asset: usize,
        #[arg(long, default_value_t = 64)]
        max_markets_per_asset: usize,
        #[arg(long, default_value_t = 15)]
        min_minutes_to_settlement: i64,
        #[arg(long, default_value_t = 1_000.0)]
        min_liquidity_usd: f64,
        #[arg(long, default_value_t = 1_000.0)]
        min_volume_24h_usd: f64,
        #[arg(long)]
        output: Option<String>,
        #[arg(long)]
        report_json: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq, Hash)]
pub enum MarketAsset {
    Btc,
    Eth,
    Sol,
    Xrp,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::{error::ErrorKind, Parser};

    #[test]
    fn monitor_defaults_to_copy_friendly_mode() {
        let cli = Cli::try_parse_from(["polyalpha-cli", "monitor", "--socket", "/tmp/custom.sock"])
            .expect("monitor args should parse");

        match cli.command {
            Command::Monitor {
                socket,
                mouse_capture,
                no_alternate_screen,
                ..
            } => {
                assert_eq!(socket, "/tmp/custom.sock");
                assert!(!mouse_capture);
                assert!(!no_alternate_screen);
            }
            other => panic!("expected monitor command, got {other:?}"),
        }
    }

    #[test]
    fn monitor_explicit_flags_override_defaults() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "monitor",
            "--socket",
            "/tmp/custom.sock",
            "--mouse-capture",
            "--no-alternate-screen",
        ])
        .expect("monitor args should parse");

        match cli.command {
            Command::Monitor {
                socket,
                mouse_capture,
                no_alternate_screen,
                ..
            } => {
                assert_eq!(socket, "/tmp/custom.sock");
                assert!(mouse_capture);
                assert!(no_alternate_screen);
            }
            other => panic!("expected monitor command, got {other:?}"),
        }
    }

    #[test]
    fn monitor_root_supports_json_interval_and_market_flags() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "monitor",
            "--json",
            "--interval-ms",
            "250",
            "--market",
            "sol-dip-80-mar",
        ])
        .expect("monitor json args should parse");

        match cli.command {
            Command::Monitor {
                json,
                snapshot,
                interval_ms,
                market,
                ..
            } => {
                assert!(json);
                assert!(!snapshot);
                assert_eq!(interval_ms, 250);
                assert_eq!(market.as_deref(), Some("sol-dip-80-mar"));
            }
            other => panic!("expected monitor command, got {other:?}"),
        }
    }

    #[test]
    fn monitor_root_supports_snapshot_and_market_flags() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "monitor",
            "--snapshot",
            "--market",
            "eth-reach-2400-mar",
        ])
        .expect("monitor snapshot args should parse");

        match cli.command {
            Command::Monitor {
                json,
                snapshot,
                market,
                ..
            } => {
                assert!(!json);
                assert!(snapshot);
                assert_eq!(market.as_deref(), Some("eth-reach-2400-mar"));
            }
            other => panic!("expected monitor command, got {other:?}"),
        }
    }

    #[test]
    fn monitor_root_rejects_json_and_snapshot_together() {
        let err = Cli::try_parse_from(["polyalpha-cli", "monitor", "--json", "--snapshot"])
            .expect_err("monitor args should reject multiple output modes");

        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parses_audit_trade_timeline_subcommand() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "audit",
            "trade-timeline",
            "--env",
            "prod",
            "--session-id",
            "session-1",
            "--trade",
            "2",
        ])
        .expect("audit trade-timeline command should parse");

        match cli.command {
            Command::Audit { .. } => {}
            other => panic!("expected audit command, got {other:?}"),
        }
    }

    #[test]
    fn parses_audit_open_decision_subcommand() {
        let cli = Cli::try_parse_from([
            "polyalpha-cli",
            "audit",
            "open-decision",
            "--env",
            "prod",
            "--session-id",
            "session-1",
            "--entry",
            "2",
        ])
        .expect("audit open-decision command should parse");

        match cli.command {
            Command::Audit { .. } => {}
            other => panic!("expected audit command, got {other:?}"),
        }
    }
}
