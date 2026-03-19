use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "polyalpha-cli", about = "PolyAlpha MVP CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    CheckConfig {
        #[arg(long, default_value = "default")]
        env: String,
    },
    Demo {
        #[arg(long, default_value = "default")]
        env: String,
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
        #[arg(long, default_value_t = 100_000.0)]
        initial_capital: f64,
        #[arg(long, default_value_t = 360)]
        rolling_window: usize,
        #[arg(long, default_value_t = 2.0)]
        entry_z: f64,
        #[arg(long, default_value_t = 0.5)]
        exit_z: f64,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long, default_value_t = 0.25)]
        max_capital_usage: f64,
        #[arg(long, default_value_t = 1.0)]
        cex_hedge_ratio: f64,
        #[arg(long, default_value_t = 0.10)]
        cex_margin_ratio: f64,
        #[arg(long, default_value_t = 2.0)]
        poly_fee_bps: f64,
        #[arg(long, default_value_t = 10.0)]
        poly_slippage_bps: f64,
        #[arg(long, default_value_t = 2.0)]
        cex_fee_bps: f64,
        #[arg(long, default_value_t = 10.0)]
        cex_slippage_bps: f64,
        #[arg(long, default_value_t = 1.0)]
        entry_fill_ratio: f64,
        #[arg(long)]
        report_json: Option<String>,
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
        #[arg(long, default_value_t = 100_000.0)]
        initial_capital: f64,
        #[arg(long, default_value_t = 360)]
        rolling_window: usize,
        #[arg(long, default_value_t = 2.0)]
        entry_z: f64,
        #[arg(long, default_value_t = 0.5)]
        exit_z: f64,
        #[arg(long)]
        position_notional_usd: Option<f64>,
        #[arg(long, default_value_t = 0.25)]
        max_capital_usage: f64,
        #[arg(long, default_value_t = 1.0)]
        cex_hedge_ratio: f64,
        #[arg(long, default_value_t = 0.10)]
        cex_margin_ratio: f64,
        #[arg(long, value_enum)]
        preset: Option<RustStressPreset>,
        #[arg(long)]
        report_json: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum MarketCommand {
    DiscoverBtc {
        #[arg(long, default_value = "default")]
        env: String,
        #[arg(long, default_value_t = 0)]
        template_market_index: usize,
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
}
