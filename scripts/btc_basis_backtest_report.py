#!/usr/bin/env python3
"""
BTC 基差回测与报告脚本。

子命令:
1) inspect-db: 检查 DuckDB 元数据、时间范围和行数
2) run: 调用 Rust replay 事实源，并输出中文报表
3) run-current-noband: 调用 Rust replay 并禁用价格带，便于对照分析
4) run-legacy: 运行旧版 Python 回测，仅供历史对照
5) walk-forward: 运行旧版 Python 样本外评估，仅供历史对照

依赖:
    python3 -m pip install -r requirements/backtest-db.txt
"""

from __future__ import annotations

import argparse
from bisect import bisect_left
import csv
import json
import math
import os
import re
import subprocess
import tempfile
import tomllib
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

try:
    import duckdb  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover
    missing = exc.name or "dependency"
    raise SystemExit(
        f"missing Python dependency `{missing}`; run "
        "`python3 -m pip install -r requirements/backtest-db.txt` first"
    ) from exc


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG_PATH = os.path.join(REPO_ROOT, "config", "default.toml")
DEFAULT_DB_PATH = "data/btc_basis_backtest_price_only_ready.duckdb"
DEFAULT_CEX_HEDGE_RATIO = 1.0
DEFAULT_CEX_MARGIN_RATIO = 0.10
DEFAULT_FEE_BPS = 2.0
DEFAULT_TRAIN_DAYS = 30
DEFAULT_TEST_DAYS = 7
DEFAULT_STEP_DAYS = 7
LINE_WIDTH = 78
MIN_MINUTES_TO_EXPIRY = 1.0
MIN_VOLATILITY = 1e-6
DELTA_BUMP_PCT = 1e-4
SQRT_TWO = math.sqrt(2.0)
MIN_WALK_FORWARD_SLICES = 1
RESOLUTION_EPSILON = 1e-4
MONEY_LITERAL_RE = r"([0-9][0-9,]*(?:\.\d+)?(?:[kmb])?)\b"
RUST_REPLAY_FACT_SOURCE = "rust_replay"
LEGACY_REFERENCE_NOTE = (
    "旧版 Python 回测保留在 `run-legacy` / `walk-forward`，仅供历史对照，"
    "不再作为默认事实源。"
)


@dataclass(frozen=True)
class RuntimeBacktestDefaults:
    initial_capital: float
    rolling_window: int
    entry_z: float
    exit_z: float
    position_notional_usd: float
    max_capital_usage: float
    poly_slippage_bps: float
    cex_slippage_bps: float
    band_policy: str
    min_poly_price: float | None
    max_poly_price: float | None


def _to_float(value: Any, default: float) -> float:
    if value is None:
        return default
    return float(value)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def load_runtime_backtest_defaults() -> RuntimeBacktestDefaults:
    with open(DEFAULT_CONFIG_PATH, "rb") as f:
        raw = tomllib.load(f)

    strategy = raw.get("strategy", {})
    basis = strategy.get("basis", {})
    risk = raw.get("risk", {})
    paper = raw.get("paper", {})
    paper_slippage = raw.get("paper_slippage", {})

    initial_capital = _to_float(paper.get("initial_capital"), 10_000.0)
    max_total_exposure = _to_float(risk.get("max_total_exposure_usd"), initial_capital)
    if abs(initial_capital) <= 1e-12:
        max_capital_usage = 0.0
    else:
        max_capital_usage = max(max_total_exposure / initial_capital, 0.0)

    return RuntimeBacktestDefaults(
        initial_capital=initial_capital,
        rolling_window=max(int(_to_float(basis.get("rolling_window_secs"), 36_000.0) / 60), 2),
        entry_z=_to_float(basis.get("entry_z_score_threshold"), 4.0),
        exit_z=_to_float(basis.get("exit_z_score_threshold"), 0.5),
        position_notional_usd=_to_float(basis.get("max_position_usd"), 200.0),
        max_capital_usage=max_capital_usage,
        poly_slippage_bps=_to_float(paper_slippage.get("poly_slippage_bps"), 50.0),
        cex_slippage_bps=_to_float(paper_slippage.get("cex_slippage_bps"), 2.0),
        band_policy=str(basis.get("band_policy", "configured_band")),
        min_poly_price=_to_optional_float(basis.get("min_poly_price")),
        max_poly_price=_to_optional_float(basis.get("max_poly_price")),
    )


RUNTIME_BACKTEST_DEFAULTS = load_runtime_backtest_defaults()
DEFAULT_INITIAL_CAPITAL = RUNTIME_BACKTEST_DEFAULTS.initial_capital
DEFAULT_WINDOW = RUNTIME_BACKTEST_DEFAULTS.rolling_window
DEFAULT_ENTRY_Z = RUNTIME_BACKTEST_DEFAULTS.entry_z
DEFAULT_EXIT_Z = RUNTIME_BACKTEST_DEFAULTS.exit_z
DEFAULT_POSITION_NOTIONAL_USD = RUNTIME_BACKTEST_DEFAULTS.position_notional_usd
DEFAULT_MAX_CAPITAL_USAGE = RUNTIME_BACKTEST_DEFAULTS.max_capital_usage
DEFAULT_SLIPPAGE_BPS = RUNTIME_BACKTEST_DEFAULTS.poly_slippage_bps


@dataclass
class EquityPoint:
    ts_ms: int
    equity: float
    cash: float
    available_cash: float
    unrealized_pnl: float
    gross_exposure: float
    reserved_margin: float
    capital_in_use: float
    open_positions: int


@dataclass
class BacktestSummary:
    db_path: str
    cex_source: str | None
    market_style: str | None
    rows_processed: int
    minute_count: int
    token_count: int
    market_count: int
    event_count: int
    start_ts_ms: int | None
    end_ts_ms: int | None
    initial_capital: float
    ending_equity: float
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    poly_realized_pnl: float
    cex_realized_pnl: float
    max_drawdown: float
    max_gross_exposure: float
    max_reserved_margin: float
    max_capital_in_use: float
    max_capital_utilization: float
    trade_count: int
    poly_trade_count: int
    cex_trade_count: int
    round_trip_count: int
    winning_round_trips: int
    win_rate: float
    total_fees_paid: float
    total_slippage_paid: float
    poly_fees_paid: float
    poly_slippage_paid: float
    cex_fees_paid: float
    cex_slippage_paid: float
    annualized_sharpe: float
    strategy: dict[str, Any]
    anomaly_count: int = 0
    max_abs_anomaly_equity_jump_usd: float = 0.0
    max_anomaly_mark_share: float = 0.0
    top_anomaly_start_ts_ms: int | None = None
    top_anomaly_end_ts_ms: int | None = None
    top_anomaly_reason_codes: list[str] = field(default_factory=list)


@dataclass
class MarketPerformance:
    market_id: str
    event_id: str
    market_question: str
    rows_processed: int = 0
    trade_count: int = 0
    round_trip_count: int = 0
    winning_round_trips: int = 0
    gross_pnl: float = 0.0
    fees_paid: float = 0.0
    slippage_paid: float = 0.0

    def net_pnl(self) -> float:
        return self.gross_pnl - self.fees_paid - self.slippage_paid

    def win_rate(self) -> float:
        if self.round_trip_count <= 0:
            return 0.0
        return self.winning_round_trips / self.round_trip_count


@dataclass
class TokenState:
    event_id: str = ""
    market_id: str = ""
    position_open: bool = False
    poly_entry_price: float = 0.0
    poly_entry_notional: float = 0.0
    shares: float = 0.0
    last_price: float = 0.0
    cex_entry_price: float = 0.0
    cex_entry_notional_usd: float = 0.0
    cex_qty_btc: float = 0.0
    reserved_margin: float = 0.0
    last_cex_price: float = 0.0
    last_signal_basis: float = 0.0
    has_last_price: bool = False
    history: deque[float] | None = None
    round_trip_pnl: float = 0.0


@dataclass(frozen=True)
class MarketRule:
    kind: str
    lower_strike: float | None = None
    upper_strike: float | None = None


@dataclass(frozen=True)
class ContractSpec:
    event_id: str
    market_id: str
    token_id: str
    token_side: str
    market_question: str
    start_ts_ms: int
    end_ts_ms: int
    rule: MarketRule
    resolved_payout: float | None = None


@dataclass(frozen=True)
class BacktestInputs:
    metadata: dict[str, str]
    market_labels: dict[str, tuple[str, str]]
    contract_specs: dict[str, ContractSpec]
    observations_by_minute: dict[int, list["PolyObservation"]]


@dataclass(frozen=True)
class WalkForwardSliceResult:
    slice_index: int
    train_start_ts_ms: int
    train_end_ts_ms: int
    test_start_ts_ms: int
    test_end_ts_ms: int
    train_summary: BacktestSummary
    test_summary: BacktestSummary


@dataclass(frozen=True)
class PolyObservation:
    ts_ms: int
    event_id: str
    market_id: str
    token_id: str
    token_side: str
    poly_price: float
    cex_price: float
    sigma: float | None
    contract: ContractSpec


@dataclass(frozen=True)
class SignalSnapshot:
    observation: PolyObservation
    fair_value: float
    signal_basis: float
    zscore: float | None
    delta: float


@dataclass
class RunConfig:
    db_path: str
    market_id: str | None
    token_id: str | None
    start_ts_ms: int | None
    end_ts_ms: int | None
    trade_start_ts_ms: int | None
    initial_capital: float
    rolling_window: int
    entry_z: float
    exit_z: float
    position_units: float | None
    position_notional_usd: float | None
    max_capital_usage: float
    cex_hedge_ratio: float
    cex_margin_ratio: float
    fee_bps: float
    slippage_bps: float
    poly_slippage_bps: float
    cex_slippage_bps: float
    min_poly_price: float | None
    max_poly_price: float | None
    report_json: str | None
    equity_csv: str | None


@dataclass(frozen=True)
class ReportMarketRow:
    market_id: str
    event_id: str
    market_question: str
    entry_count: int
    signal_count: int
    rejected_capital_count: int
    net_pnl: float


def print_title(title: str) -> None:
    print("=" * LINE_WIDTH)
    print(title)
    print("=" * LINE_WIDTH)


def print_section(title: str) -> None:
    print()
    print(f"[{title}]")


def print_kv(label: str, value: Any) -> None:
    print(f"  {label}: {value}")


def fmt_int(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def fmt_money(value: float | None, signed: bool = False) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed and value > 0 else ""
    return f"{prefix}{value:,.4f} USD"


def fmt_pct(value: float | None, signed: bool = False) -> str:
    if value is None:
        return "-"
    pct_value = value * 100.0
    prefix = "+" if signed and pct_value > 0 else ""
    return f"{prefix}{pct_value:.4f}%"


def fmt_ratio(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def fmt_ts_ms(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "-"
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_period(start_ts_ms: int | None, end_ts_ms: int | None) -> str:
    if start_ts_ms is None or end_ts_ms is None:
        return "-"
    return f"{fmt_ts_ms(start_ts_ms)} -> {fmt_ts_ms(end_ts_ms)}"


def fmt_optional_text(value: str | None, fallback: str = "全部") -> str:
    if value is None or not str(value).strip():
        return fallback
    return str(value)


def fmt_filter_time(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "自动"
    return fmt_ts_ms(ts_ms)


def metadata_label(key: str) -> str:
    labels = {
        "basis_formula": "基差定义",
        "raw_basis_formula": "原始 basis 列定义",
        "signal_basis_formula": "回测信号定义",
        "cex_alignment": "CEX 对齐方式",
        "cex_source": "CEX 数据源",
        "cex_source_policy": "CEX 数据源策略",
        "discovery_source": "市场发现源",
        "market_style": "市场样式",
        "settlement_payout_source": "到期结算来源",
    }
    return labels.get(key, key)


def table_label(name: str) -> str:
    labels = {
        "gamma_events": "Gamma 事件表",
        "polymarket_contracts": "Polymarket 合约表",
        "polymarket_price_history": "Polymarket 历史价格表",
        "polymarket_history_failures": "Polymarket 抓取失败表",
        "basis_1m": "分钟级基差表",
    }
    if re.fullmatch(r"binance_[a-z0-9]+_1m", name):
        asset = name.removeprefix("binance_").removesuffix("_1m").upper()
        return f"Binance {asset} 1m K线表"
    return labels.get(name, name)


def strategy_label(key: str) -> str:
    labels = {
        "name": "策略名称",
        "rolling_window": "滚动窗口",
        "entry_z": "入场阈值",
        "exit_z": "出场阈值",
        "band_policy": "价格带策略",
        "position_sizing_mode": "仓位模式",
        "position_units": "单次 Poly 份数",
        "position_notional_usd": "单次 Poly 名义",
        "max_capital_usage": "最大资金使用率",
        "cex_hedge_ratio": "CEX 名义对冲比",
        "cex_margin_ratio": "CEX 初始保证金率",
        "fee_bps": "单腿手续费",
        "slippage_bps": "单腿滑点",
        "min_poly_price": "最小 Poly 价格",
        "max_poly_price": "最大 Poly 价格",
        "signal_basis": "信号基差",
        "cex_probability_proxy": "CEX 概率代理",
        "trade_start_ts_ms": "交易起点",
        "settlement_payout_source": "到期结算来源",
        "minute_count": "唯一分钟点数",
        "execution_source": "默认事实源",
        "report_layer_role": "Python 层职责",
        "legacy_reference_mode": "历史对照入口",
        "entry_fill_ratio": "入场成交比例",
        "pnl_scope": "收益统计范围",
        "note": "口径说明",
    }
    return labels.get(key, key)


def strategy_value_display(key: str, value: Any) -> str:
    if key == "rolling_window":
        return f"{value} 分钟"
    if key in {"entry_z", "exit_z"}:
        return fmt_ratio(float(value))
    if key == "band_policy" and value == "configured_band":
        return "使用配置价格带"
    if key == "band_policy" and value == "disabled":
        return "禁用价格带"
    if key == "position_sizing_mode" and value == "target_poly_notional_usd":
        return "按 Poly 名义金额下单"
    if key == "position_sizing_mode" and value == "legacy_poly_units":
        return "按 Poly 份数下单（兼容模式）"
    if key == "position_units":
        return f"{float(value):.4f} 份"
    if key == "position_notional_usd":
        return fmt_money(float(value))
    if key in {"max_capital_usage", "cex_margin_ratio"}:
        return fmt_pct(float(value))
    if key == "cex_hedge_ratio":
        return f"{float(value):.2f}x"
    if key in {"fee_bps", "slippage_bps"}:
        return f"{float(value):.2f} bps"
    if key in {"min_poly_price", "max_poly_price"}:
        return "-" if value is None else f"{float(value):.4f}"
    if key == "entry_fill_ratio":
        return fmt_pct(float(value))
    if key == "trade_start_ts_ms":
        return fmt_filter_time(int(value)) if value is not None else "自动"
    if key == "name" and value == "poly_basis_structural_delta_hedge":
        return "Poly + CEX 因果概率 / Delta 对冲回归"
    if key == "execution_source" and value == RUST_REPLAY_FACT_SOURCE:
        return "Rust replay 单一事实源"
    if key == "report_layer_role" and value == "python_cli_chinese_report":
        return "Python CLI 只负责中文报表与导出"
    if key == "signal_basis" and value == "poly_price - causal_terminal_probability":
        return "Poly 价格 - 因果终值概率"
    if (
        key == "cex_probability_proxy"
        and value == "lognormal terminal probability from previous completed cex bar and trailing realized volatility"
    ):
        return "基于上一根已完成 CEX bar、剩余到期时间和滚动实现波动率估算终值概率"
    if key == "pnl_scope" and value == "poly + cex combined":
        return "统计 Poly + CEX 两腿组合收益"
    if (
        key == "settlement_payout_source"
        and value
        == "prefer gamma outcomePrices when officially resolved, otherwise fallback to cex-derived terminal payout"
    ):
        return "优先使用 Polymarket 官方 outcomePrices；缺失时才回退到本地 CEX 终值近似"
    if (
        key == "note"
        and value
        == "Poly 腿按真实 premium 记账；CEX 腿按结构化概率模型的数值 delta 做方向与规模近似，并统一使用上一根已完成的 CEX bar；这仍是研究回测，不是实盘可成交收益。"
    ):
        return "Poly 腿按真实 premium 记账；CEX 腿按结构化概率模型的数值 delta 做方向与规模近似，并统一使用上一根已完成的 CEX bar；这仍是研究回测，不是实盘可成交收益。"
    return str(value)


def compact_text(text: str, max_len: int = 42) -> str:
    text = " ".join(str(text).split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def market_payload(row: MarketPerformance) -> dict[str, Any]:
    return {
        "market_id": row.market_id,
        "event_id": row.event_id,
        "market_question": row.market_question,
        "rows_processed": row.rows_processed,
        "trade_count": row.trade_count,
        "round_trip_count": row.round_trip_count,
        "winning_round_trips": row.winning_round_trips,
        "win_rate": row.win_rate(),
        "gross_pnl": row.gross_pnl,
        "fees_paid": row.fees_paid,
        "slippage_paid": row.slippage_paid,
        "net_pnl": row.net_pnl(),
    }


def add_backtest_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market-id", default=None, help="可选 market_id 过滤")
    parser.add_argument("--token-id", default=None, help="可选 token_id 过滤")
    parser.add_argument("--start", default=None, help="开始时间: ts_ms / YYYY-MM-DD / ISO8601")
    parser.add_argument("--end", default=None, help="结束时间: ts_ms / YYYY-MM-DD / ISO8601")
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=DEFAULT_INITIAL_CAPITAL,
        help="初始资金，单位 USD",
    )
    parser.add_argument(
        "--rolling-window",
        type=int,
        default=DEFAULT_WINDOW,
        help="滚动窗口，单位分钟",
    )
    parser.add_argument("--entry-z", type=float, default=DEFAULT_ENTRY_Z, help="入场 z-score")
    parser.add_argument("--exit-z", type=float, default=DEFAULT_EXIT_Z, help="出场 z-score")
    parser.add_argument(
        "--position-units",
        type=float,
        default=None,
        help="兼容模式：每次信号买入的 Poly 份数；不传时改用按美元名义下单",
    )
    parser.add_argument(
        "--position-notional-usd",
        type=float,
        default=None,
        help=f"每次信号买入的 Poly 名义金额；默认取 config/default.toml，当前为 {DEFAULT_POSITION_NOTIONAL_USD:.2f} USD",
    )
    parser.add_argument(
        "--max-capital-usage",
        type=float,
        default=DEFAULT_MAX_CAPITAL_USAGE,
        help="允许占用的最大资金比例，按 Poly premium + CEX 保证金计算",
    )
    parser.add_argument(
        "--cex-hedge-ratio",
        type=float,
        default=DEFAULT_CEX_HEDGE_RATIO,
        help="CEX 对冲名义 / Poly 名义",
    )
    parser.add_argument(
        "--cex-margin-ratio",
        type=float,
        default=DEFAULT_CEX_MARGIN_RATIO,
        help="CEX 线性永续初始保证金率",
    )
    parser.add_argument("--fee-bps", type=float, default=DEFAULT_FEE_BPS, help="单边手续费 bps")
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=DEFAULT_SLIPPAGE_BPS,
        help="共享回测口径的单边滑点 bps；默认与 Poly 腿一致",
    )
    parser.add_argument(
        "--poly-slippage-bps",
        type=float,
        default=RUNTIME_BACKTEST_DEFAULTS.poly_slippage_bps,
        help="Rust replay 默认事实源的 Poly 单边滑点 bps",
    )
    parser.add_argument(
        "--cex-slippage-bps",
        type=float,
        default=RUNTIME_BACKTEST_DEFAULTS.cex_slippage_bps,
        help="Rust replay 默认事实源的 CEX 单边滑点 bps",
    )
    parser.add_argument(
        "--min-poly-price",
        type=float,
        default=RUNTIME_BACKTEST_DEFAULTS.min_poly_price,
        help="Rust replay 默认事实源的最小 Poly 价格过滤",
    )
    parser.add_argument(
        "--max-poly-price",
        type=float,
        default=RUNTIME_BACKTEST_DEFAULTS.max_poly_price,
        help="Rust replay 默认事实源的最大 Poly 价格过滤",
    )
    parser.add_argument("--report-json", default=None, help="可选 JSON 报告输出路径")
    parser.add_argument("--equity-csv", default=None, help="可选权益曲线 CSV 输出路径")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BTC 基差回测报表 CLI")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="DuckDB 数据库路径")

    sub = parser.add_subparsers(dest="command", required=True)

    inspect_parser = sub.add_parser("inspect-db", help="检查数据库元数据与样本覆盖情况")
    inspect_parser.add_argument("--show-failures", action="store_true", help="显示抓取失败样例")

    run_parser = sub.add_parser("run", help="调用 Rust replay 并输出中文报表（默认事实源）")
    add_backtest_run_arguments(run_parser)

    noband_run_parser = sub.add_parser(
        "run-current-noband",
        help="调用 Rust replay 并禁用价格带，用于 current/current-noband 对照",
    )
    add_backtest_run_arguments(noband_run_parser)

    legacy_run_parser = sub.add_parser("run-legacy", help="运行旧版 Python 回测，仅供历史对照")
    add_backtest_run_arguments(legacy_run_parser)

    walk_parser = sub.add_parser("walk-forward", help="旧版 Python 样本外评估，仅供历史对照")
    add_backtest_run_arguments(walk_parser)
    walk_parser.add_argument(
        "--train-days",
        type=int,
        default=DEFAULT_TRAIN_DAYS,
        help="训练窗天数，按对齐后的唯一分钟点滚动切片",
    )
    walk_parser.add_argument(
        "--test-days",
        type=int,
        default=DEFAULT_TEST_DAYS,
        help="测试窗天数，按对齐后的唯一分钟点滚动切片",
    )
    walk_parser.add_argument(
        "--step-days",
        type=int,
        default=DEFAULT_STEP_DAYS,
        help="窗口推进天数；默认每次推进一个测试窗",
    )
    walk_parser.add_argument(
        "--max-slices",
        type=int,
        default=0,
        help="最多生成多少个滚动切片；0 表示不限制",
    )

    return parser


def parse_time_to_ms(raw: str | None) -> int | None:
    if raw is None:
        return None

    text = raw.strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    dt: datetime | None = None
    try:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        pass

    if dt is None:
        candidate = text
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        dt = dt.astimezone(UTC)

    return int(dt.timestamp() * 1000)


def fetch_metadata(conn: Any) -> dict[str, str]:
    try:
        rows = conn.execute("select key, value from build_metadata").fetchall()
    except Exception:
        return {}
    return {str(key): str(value) for key, value in rows}


def parse_json_string_array(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return [str(parsed)]
    return [str(raw)]


def parse_outcome_prices(raw: Any) -> list[float]:
    values: list[float] = []
    for item in parse_json_string_array(raw):
        try:
            values.append(float(item))
        except (TypeError, ValueError):
            continue
    return values


def clamp_probability_value(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def is_official_resolution(outcome_prices: list[float], status: str) -> bool:
    normalized_status = status.strip().lower()
    if normalized_status in {"resolved", "finalized", "settled"}:
        return True
    if len(outcome_prices) < 2:
        return False
    if abs(sum(outcome_prices[:2]) - 1.0) > RESOLUTION_EPSILON:
        return False
    return all(
        abs(price) <= RESOLUTION_EPSILON or abs(price - 1.0) <= RESOLUTION_EPSILON
        for price in outcome_prices[:2]
    )


def load_market_resolutions(conn: Any) -> dict[tuple[str, str], float]:
    try:
        rows = conn.execute("select raw_json from gamma_events").fetchall()
    except Exception:
        return {}

    payouts: dict[tuple[str, str], float] = {}
    for (raw_json,) in rows:
        payload: Any
        if isinstance(raw_json, dict):
            payload = raw_json
        else:
            try:
                payload = json.loads(str(raw_json))
            except (TypeError, json.JSONDecodeError):
                continue

        if not isinstance(payload, dict):
            continue

        for market in payload.get("markets", []) or []:
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id", "")).strip()
            if not market_id:
                continue

            outcome_prices = parse_outcome_prices(market.get("outcomePrices"))
            status = str(market.get("umaResolutionStatus", ""))
            if not is_official_resolution(outcome_prices, status):
                continue
            if len(outcome_prices) < 2:
                continue

            payouts[(market_id, "yes")] = clamp_probability_value(outcome_prices[0])
            payouts[(market_id, "no")] = clamp_probability_value(outcome_prices[1])
    return payouts


def load_market_labels(conn: Any) -> dict[str, tuple[str, str]]:
    try:
        rows = conn.execute(
            """
            select
                market_id,
                min(event_id) as event_id,
                min(market_question) as market_question
            from polymarket_contracts
            group by market_id
            """
        ).fetchall()
    except Exception:
        return {}
    return {
        str(market_id): (str(event_id), str(market_question))
        for market_id, event_id, market_question in rows
    }


def inspect_db(db_path: str, show_failures: bool) -> None:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        metadata = fetch_metadata(conn)
        cex_table = detect_cex_table_name(conn)
        table_names = [
            "gamma_events",
            "polymarket_contracts",
            "polymarket_price_history",
            "polymarket_history_failures",
            cex_table,
            "basis_1m",
        ]
        counts = {
            name: conn.execute(f"select count(*) from {name}").fetchone()[0]
            for name in table_names
        }
        range_row = conn.execute(
            """
            select
                cast(min(ts_utc) as varchar),
                cast(max(ts_utc) as varchar),
                min(ts_ms),
                max(ts_ms),
                count(distinct event_id),
                count(distinct market_id),
                count(distinct token_id)
            from basis_1m
            """
        ).fetchone()
        print_title("基差回测数据库检查")

        print_section("数据库")
        print_kv("数据库路径", db_path)
        print_kv("CEX 1m 表", cex_table)
        if metadata:
            print_section("构建元数据")
            for key in sorted(metadata.keys()):
                print_kv(metadata_label(key), metadata[key])
        else:
            print_section("构建元数据")
            print_kv("状态", "空")

        print_section("当前回测引擎")
        print_kv(
            "run 子命令口径",
            "直接读取原始 Polymarket/Binance 表，按上一根已完成 CEX bar + 因果概率模型现场回放；不依赖旧 basis_1m 信号定义",
        )

        print_section("数据表行数")
        for name in table_names:
            print_kv(table_label(name), fmt_int(counts[name]))

        print_section("样本覆盖")
        print_kv("起始时间", range_row[0])
        print_kv("结束时间", range_row[1])
        print_kv("起始时间戳", fmt_int(range_row[2]))
        print_kv("结束时间戳", fmt_int(range_row[3]))
        print_kv("覆盖事件数", fmt_int(range_row[4]))
        print_kv("覆盖市场数", fmt_int(range_row[5]))
        print_kv("覆盖 token 数", fmt_int(range_row[6]))
        print_kv("分钟级基差点数", fmt_int(counts["basis_1m"]))
        print_kv("历史抓取失败数", fmt_int(counts["polymarket_history_failures"]))

        if show_failures and counts["polymarket_history_failures"] > 0:
            failures = conn.execute(
                """
                select market_id, token_id, token_side, error_type, error_message
                from polymarket_history_failures
                order by market_id, token_side
                limit 20
                """
            ).fetchall()
            print_section("抓取失败样例")
            for idx, row in enumerate(failures, start=1):
                print(f"  {idx}. market_id={row[0]} token_id={row[1]} side={row[2]}")
                print(f"     错误类型: {row[3]}")
                print(f"     错误信息: {row[4]}")
    finally:
        conn.close()


def build_row_where_clause(cfg: RunConfig) -> tuple[str, list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if cfg.market_id:
        where_clauses.append("market_id = ?")
        params.append(cfg.market_id)
    if cfg.token_id:
        where_clauses.append("token_id = ?")
        params.append(cfg.token_id)
    if cfg.start_ts_ms is not None:
        where_clauses.append("ts_ms >= ?")
        params.append(cfg.start_ts_ms)
    if cfg.end_ts_ms is not None:
        where_clauses.append("ts_ms <= ?")
        params.append(cfg.end_ts_ms)

    where_sql = ""
    if where_clauses:
        where_sql = "where " + " and ".join(where_clauses)

    return where_sql, params


def build_contract_where_clause(cfg: RunConfig) -> tuple[str, list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []

    if cfg.market_id:
        where_clauses.append("market_id = ?")
        params.append(cfg.market_id)
    if cfg.token_id:
        where_clauses.append("token_id = ?")
        params.append(cfg.token_id)
    if cfg.start_ts_ms is not None:
        where_clauses.append("(end_ts * 1000) >= ?")
        params.append(cfg.start_ts_ms)
    if cfg.end_ts_ms is not None:
        where_clauses.append("(start_ts * 1000) <= ?")
        params.append(cfg.end_ts_ms)

    where_sql = ""
    if where_clauses:
        where_sql = "where " + " and ".join(where_clauses)
    return where_sql, params


def load_contract_specs(
    conn: Any,
    cfg: RunConfig,
    market_resolutions: dict[tuple[str, str], float],
) -> dict[str, ContractSpec]:
    where_sql, params = build_contract_where_clause(cfg)
    rows = conn.execute(
        f"""
        select
            event_id,
            market_id,
            token_id,
            token_side,
            market_question,
            start_ts,
            end_ts
        from polymarket_contracts
        {where_sql}
        order by market_id asc, token_side asc
        """,
        params,
    ).fetchall()
    specs: dict[str, ContractSpec] = {}
    for event_id, market_id, token_id, token_side, market_question, start_ts, end_ts in rows:
        specs[str(token_id)] = ContractSpec(
            event_id=str(event_id),
            market_id=str(market_id),
            token_id=str(token_id),
            token_side=str(token_side),
            market_question=str(market_question),
            start_ts_ms=int(start_ts) * 1000,
            end_ts_ms=int(end_ts) * 1000,
            rule=parse_market_rule(str(market_question)),
            resolved_payout=market_resolutions.get((str(market_id), str(token_side))),
        )
    return specs


def load_poly_rows(conn: Any, cfg: RunConfig) -> list[tuple[int, str, str, str, str, float]]:
    where_sql, params = build_row_where_clause(cfg)

    sql = f"""
        select
            ts_ms,
            event_id,
            market_id,
            token_id,
            token_side,
            poly_price
        from polymarket_price_history
        {where_sql}
        order by ts_ms asc, token_id asc
    """
    return conn.execute(sql, params).fetchall()


def detect_cex_table_name(conn: Any) -> str:
    tables = [
        str(row[0])
        for row in conn.execute("show tables").fetchall()
        if re.fullmatch(r"binance_[a-z0-9]+_1m", str(row[0]))
    ]
    if not tables:
        raise SystemExit("no binance_*_1m table found in database")
    if len(tables) > 1:
        raise SystemExit(f"multiple binance_*_1m tables found: {tables}")
    return tables[0]


def load_cex_rows(conn: Any) -> list[tuple[int, float]]:
    cex_table = detect_cex_table_name(conn)
    return conn.execute(
        """
        select ts_ms, close
        from {}
        order by ts_ms asc
        """.format(cex_table)
    ).fetchall()


def prepare_backtest_inputs(cfg: RunConfig) -> BacktestInputs:
    conn = duckdb.connect(cfg.db_path, read_only=True)
    try:
        metadata = fetch_metadata(conn)
        market_labels = load_market_labels(conn)
        market_resolutions = load_market_resolutions(conn)
        contract_specs = load_contract_specs(conn, cfg, market_resolutions)
        poly_rows = load_poly_rows(conn, cfg)
        cex_rows = load_cex_rows(conn)
    finally:
        conn.close()

    if not contract_specs:
        raise SystemExit("no contract rows matched filter; try inspect-db first")
    if not poly_rows:
        raise SystemExit("no polymarket price rows matched filter; try inspect-db first")
    if not cex_rows:
        raise SystemExit("no cex rows available in database; try prepare-db first")

    cex_close_by_minute = {int(ts_ms): float(close) for ts_ms, close in cex_rows}
    sigma_by_minute = build_rolling_volatility_by_minute(cex_rows, cfg.rolling_window)
    observations_by_minute = build_observations_by_minute(
        poly_rows,
        contract_specs,
        cex_close_by_minute,
        sigma_by_minute,
    )
    if not observations_by_minute:
        raise SystemExit("no causally aligned observations matched filter; try inspect-db first")

    return BacktestInputs(
        metadata=metadata,
        market_labels=market_labels,
        contract_specs=contract_specs,
        observations_by_minute=observations_by_minute,
    )


def parse_money(raw: str) -> float:
    normalized = raw.replace(",", "").strip().lower()
    if not normalized:
        raise ValueError("empty money literal")

    multiplier = 1.0
    suffix = normalized[-1]
    if suffix.isalpha():
        # Polymarket 的 BTC 市场常写成 $122K，这里要按真实数量级还原。
        multiplier = {
            "k": 1_000.0,
            "m": 1_000_000.0,
            "b": 1_000_000_000.0,
        }.get(suffix, 0.0)
        if multiplier == 0.0:
            raise ValueError(f"unsupported money suffix: {raw}")
        normalized = normalized[:-1].strip()
    return float(normalized) * multiplier


def parse_market_rule(market_question: str) -> MarketRule:
    question = " ".join(market_question.strip().lower().split())

    between_match = re.search(
        rf"between\s+\${MONEY_LITERAL_RE}\s+and\s+\${MONEY_LITERAL_RE}",
        question,
    )
    if between_match:
        lower = parse_money(between_match.group(1))
        upper = parse_money(between_match.group(2))
        return MarketRule(kind="between", lower_strike=lower, upper_strike=upper)

    above_match = re.search(
        rf"(above|greater than)\s+\${MONEY_LITERAL_RE}",
        question,
    )
    if above_match:
        return MarketRule(kind="above", lower_strike=parse_money(above_match.group(2)))

    below_match = re.search(
        rf"(below|less than)\s+\${MONEY_LITERAL_RE}",
        question,
    )
    if below_match:
        return MarketRule(kind="below", upper_strike=parse_money(below_match.group(2)))

    raise ValueError(f"unsupported market question for causal signal model: {market_question}")


def build_rolling_volatility_by_minute(
    cex_rows: list[tuple[int, float]],
    window: int,
) -> dict[int, float | None]:
    if window <= 0:
        return {}

    out: dict[int, float | None] = {}
    returns_window: deque[float] = deque(maxlen=window)
    previous_close: float | None = None
    for ts_ms, close in cex_rows:
        close = float(close)
        if previous_close is not None and previous_close > 0.0 and close > 0.0:
            returns_window.append(math.log(close / previous_close))

        sigma = None
        if len(returns_window) >= 2:
            mean = sum(returns_window) / len(returns_window)
            variance = sum((item - mean) ** 2 for item in returns_window) / (len(returns_window) - 1)
            sigma = math.sqrt(max(variance, 0.0))
        out[int(ts_ms)] = sigma
        previous_close = close
    return out


def build_observations_by_minute(
    poly_rows: list[tuple[int, str, str, str, str, float]],
    contract_specs: dict[str, ContractSpec],
    cex_close_by_minute: dict[int, float],
    sigma_by_minute: dict[int, float | None],
) -> dict[int, list[PolyObservation]]:
    grouped: dict[int, list[PolyObservation]] = defaultdict(list)
    for ts_ms, event_id, market_id, token_id, token_side, poly_price in poly_rows:
        contract = contract_specs.get(str(token_id))
        if contract is None:
            continue

        # 用上一根已完成的 CEX bar，避免把同一分钟后半段信息带进当前决策。
        cex_ref_ts = int(ts_ms) - 60_000
        cex_price = cex_close_by_minute.get(cex_ref_ts)
        if cex_price is None:
            continue

        grouped[int(ts_ms)].append(
            PolyObservation(
                ts_ms=int(ts_ms),
                event_id=str(event_id),
                market_id=str(market_id),
                token_id=str(token_id),
                token_side=str(token_side),
                poly_price=float(poly_price),
                cex_price=float(cex_price),
                sigma=sigma_by_minute.get(cex_ref_ts),
                contract=contract,
            )
        )
    return grouped


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / SQRT_TWO))


def yes_probability_from_rule(
    rule: MarketRule,
    spot_price: float,
    sigma: float | None,
    minutes_to_expiry: float,
) -> float:
    spot = max(float(spot_price), 1e-9)
    if sigma is None or sigma < MIN_VOLATILITY or minutes_to_expiry <= MIN_MINUTES_TO_EXPIRY:
        return realized_yes_payout(rule, spot)

    variance = max((sigma**2) * minutes_to_expiry, MIN_VOLATILITY**2)
    stdev = math.sqrt(variance)
    mu = math.log(spot) - 0.5 * variance

    def above_probability(strike: float) -> float:
        z_value = (math.log(max(strike, 1e-9)) - mu) / stdev
        return 1.0 - normal_cdf(z_value)

    if rule.kind == "above":
        return above_probability(float(rule.lower_strike))
    if rule.kind == "below":
        return 1.0 - above_probability(float(rule.upper_strike))
    if rule.kind == "between":
        lower = above_probability(float(rule.lower_strike))
        upper = above_probability(float(rule.upper_strike))
        return min(1.0, max(0.0, lower - upper))
    raise ValueError(f"unsupported market rule: {rule.kind}")


def realized_yes_payout(rule: MarketRule, terminal_price: float) -> float:
    price = float(terminal_price)
    if rule.kind == "above":
        return 1.0 if price > float(rule.lower_strike) else 0.0
    if rule.kind == "below":
        return 1.0 if price < float(rule.upper_strike) else 0.0
    if rule.kind == "between":
        lower = float(rule.lower_strike)
        upper = float(rule.upper_strike)
        return 1.0 if lower <= price < upper else 0.0
    raise ValueError(f"unsupported market rule: {rule.kind}")


def token_fair_value(
    contract: ContractSpec,
    spot_price: float,
    sigma: float | None,
    minutes_to_expiry: float,
) -> float:
    yes_probability = yes_probability_from_rule(contract.rule, spot_price, sigma, minutes_to_expiry)
    if contract.token_side.lower() == "yes":
        return min(1.0, max(0.0, yes_probability))
    return min(1.0, max(0.0, 1.0 - yes_probability))


def token_delta(
    contract: ContractSpec,
    spot_price: float,
    sigma: float | None,
    minutes_to_expiry: float,
) -> float:
    bump = max(abs(spot_price) * DELTA_BUMP_PCT, 1.0)
    lower_spot = max(spot_price - bump, 1e-9)
    upper_spot = spot_price + bump
    down = token_fair_value(contract, lower_spot, sigma, minutes_to_expiry)
    up = token_fair_value(contract, upper_spot, sigma, minutes_to_expiry)
    return (up - down) / (upper_spot - lower_spot)


def rolling_zscore(values: deque[float], latest: float) -> float | None:
    n = len(values)
    if n < 2:
        return None

    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std = math.sqrt(variance)
    if std <= 1e-12:
        return None
    return (latest - mean) / std


def apply_trade_costs(cash: float, trade_notional: float, fee_bps: float, slippage_bps: float) -> tuple[float, float, float]:
    notional = abs(trade_notional)
    fee_paid = notional * fee_bps / 10_000.0
    slippage_paid = notional * slippage_bps / 10_000.0
    return cash - fee_paid - slippage_paid, fee_paid, slippage_paid


def resolve_position_size(cfg: RunConfig, poly_price: float) -> tuple[float, float, str]:
    if poly_price <= 1e-12:
        return 0.0, 0.0, "target_poly_notional_usd"

    if cfg.position_units is not None:
        shares = max(cfg.position_units, 0.0)
        return shares, shares * poly_price, "legacy_poly_units"

    target_notional = cfg.position_notional_usd
    if target_notional is None:
        target_notional = DEFAULT_POSITION_NOTIONAL_USD
    target_notional = max(target_notional, 0.0)
    shares = target_notional / poly_price
    return shares, target_notional, "target_poly_notional_usd"


def linear_cex_pnl(entry_price: float, exit_price: float, qty_btc: float) -> float:
    if entry_price <= 1e-12 or abs(qty_btc) <= 1e-12:
        return 0.0
    return qty_btc * (exit_price - entry_price)


def run_backtest(cfg: RunConfig) -> tuple[BacktestSummary, list[EquityPoint], list[MarketPerformance]]:
    inputs = prepare_backtest_inputs(cfg)
    metadata = inputs.metadata
    market_labels = inputs.market_labels
    contract_specs = inputs.contract_specs
    observations_by_minute = inputs.observations_by_minute
    minute_keys = sorted(observations_by_minute)
    trade_start_ts_ms = cfg.trade_start_ts_ms if cfg.trade_start_ts_ms is not None else cfg.start_ts_ms
    evaluation_minute_keys = [
        ts_ms for ts_ms in minute_keys if trade_start_ts_ms is None or ts_ms >= trade_start_ts_ms
    ]
    if not evaluation_minute_keys:
        raise SystemExit("no evaluation minutes matched filter; try inspect-db first")

    token_states: dict[str, TokenState] = {}
    market_stats: dict[str, MarketPerformance] = {}
    cash = cfg.initial_capital
    equity_curve: list[EquityPoint] = []

    all_events: set[str] = set()
    all_markets: set[str] = set()
    all_tokens: set[str] = set()

    trade_count = 0
    poly_trade_count = 0
    cex_trade_count = 0
    round_trip_count = 0
    winning_round_trips = 0
    poly_realized_gross = 0.0
    cex_realized_gross = 0.0
    total_fees = 0.0
    total_slippage = 0.0
    poly_fees_paid = 0.0
    poly_slippage_paid = 0.0
    cex_fees_paid = 0.0
    cex_slippage_paid = 0.0
    max_gross_exposure = 0.0
    max_reserved_margin = 0.0
    max_capital_in_use = 0.0
    max_capital_utilization = 0.0

    peak_equity = cfg.initial_capital
    max_drawdown = 0.0
    resolved_position_mode = "legacy_poly_units" if cfg.position_units is not None else "target_poly_notional_usd"
    resolved_position_notional = cfg.position_notional_usd
    if resolved_position_mode == "target_poly_notional_usd" and resolved_position_notional is None:
        resolved_position_notional = DEFAULT_POSITION_NOTIONAL_USD

    def ensure_market_stat(market_id: str, event_id: str) -> MarketPerformance:
        row = market_stats.get(market_id)
        if row is not None:
            return row

        label_event_id, market_question = market_labels.get(market_id, (event_id, ""))
        row = MarketPerformance(
            market_id=market_id,
            event_id=label_event_id or event_id,
            market_question=market_question or "",
        )
        market_stats[market_id] = row
        return row

    def portfolio_usage() -> tuple[float, float]:
        reserved = sum(state.reserved_margin for state in token_states.values() if state.position_open)
        capital = sum(
            state.poly_entry_notional + state.reserved_margin
            for state in token_states.values()
            if state.position_open
        )
        return reserved, capital

    def close_position(
        token_id: str,
        current_cex_price: float,
        *,
        market_exit_price: float | None = None,
        settle_to_payout: bool = False,
    ) -> None:
        nonlocal cash
        nonlocal poly_realized_gross
        nonlocal cex_realized_gross
        nonlocal total_fees
        nonlocal total_slippage
        nonlocal poly_fees_paid
        nonlocal poly_slippage_paid
        nonlocal cex_fees_paid
        nonlocal cex_slippage_paid
        nonlocal trade_count
        nonlocal poly_trade_count
        nonlocal cex_trade_count
        nonlocal round_trip_count
        nonlocal winning_round_trips

        state = token_states[token_id]
        if not state.position_open:
            return

        contract = contract_specs[token_id]
        market_stat = ensure_market_stat(contract.market_id, contract.event_id)
        exit_cex_price = float(current_cex_price)

        if settle_to_payout:
            payout = contract.resolved_payout
            if payout is None:
                payout = token_fair_value(contract, exit_cex_price, None, 0.0)
            sale_value = state.shares * payout
            poly_gross_pnl = sale_value - state.poly_entry_notional
        else:
            sale_price = float(market_exit_price if market_exit_price is not None else state.last_price)
            sale_value = state.shares * sale_price
            poly_gross_pnl = state.shares * (sale_price - state.poly_entry_price)

        cash += sale_value
        poly_realized_gross += poly_gross_pnl
        state.round_trip_pnl += poly_gross_pnl
        market_stat.gross_pnl += poly_gross_pnl

        if not settle_to_payout:
            cash, poly_fee_paid, poly_slippage_paid_trade = apply_trade_costs(
                cash,
                sale_value,
                cfg.fee_bps,
                cfg.slippage_bps,
            )
            total_fees += poly_fee_paid
            total_slippage += poly_slippage_paid_trade
            poly_fees_paid += poly_fee_paid
            poly_slippage_paid += poly_slippage_paid_trade
            state.round_trip_pnl -= poly_fee_paid + poly_slippage_paid_trade
            trade_count += 1
            poly_trade_count += 1
            market_stat.trade_count += 1
            market_stat.fees_paid += poly_fee_paid
            market_stat.slippage_paid += poly_slippage_paid_trade

        cex_gross_pnl = linear_cex_pnl(state.cex_entry_price, exit_cex_price, state.cex_qty_btc)
        cash += cex_gross_pnl
        cex_realized_gross += cex_gross_pnl
        state.round_trip_pnl += cex_gross_pnl
        market_stat.gross_pnl += cex_gross_pnl
        exit_cex_notional = abs(state.cex_qty_btc) * exit_cex_price
        if exit_cex_notional > 0.0:
            cash, cex_fee_paid, cex_slippage_paid_trade = apply_trade_costs(
                cash,
                exit_cex_notional,
                cfg.fee_bps,
                cfg.slippage_bps,
            )
            total_fees += cex_fee_paid
            total_slippage += cex_slippage_paid_trade
            cex_fees_paid += cex_fee_paid
            cex_slippage_paid += cex_slippage_paid_trade
            state.round_trip_pnl -= cex_fee_paid + cex_slippage_paid_trade
            trade_count += 1
            cex_trade_count += 1
            market_stat.trade_count += 1
            market_stat.fees_paid += cex_fee_paid
            market_stat.slippage_paid += cex_slippage_paid_trade

        round_trip_count += 1
        market_stat.round_trip_count += 1
        if state.round_trip_pnl > 0.0:
            winning_round_trips += 1
            market_stat.winning_round_trips += 1

        state.round_trip_pnl = 0.0
        state.position_open = False
        state.poly_entry_price = 0.0
        state.poly_entry_notional = 0.0
        state.shares = 0.0
        state.cex_entry_price = 0.0
        state.cex_entry_notional_usd = 0.0
        state.cex_qty_btc = 0.0
        state.reserved_margin = 0.0

    for ts_ms in minute_keys:
        batch = observations_by_minute[ts_ms]
        current_cex_price = batch[0].cex_price
        is_evaluation_minute = trade_start_ts_ms is None or ts_ms >= trade_start_ts_ms

        for state in token_states.values():
            if state.position_open:
                state.last_cex_price = current_cex_price

        signal_snapshots: list[SignalSnapshot] = []
        exit_candidates: list[SignalSnapshot] = []
        entry_candidates: list[SignalSnapshot] = []
        exited_market_ids: set[str] = set()

        for observation in batch:
            contract = observation.contract

            state = token_states.get(contract.token_id)
            if state is None:
                state = TokenState(
                    event_id=contract.event_id,
                    market_id=contract.market_id,
                    history=deque(maxlen=cfg.rolling_window),
                )
                token_states[contract.token_id] = state

            state.last_price = observation.poly_price
            state.last_cex_price = current_cex_price
            state.has_last_price = True

            minutes_to_expiry = max((contract.end_ts_ms - observation.ts_ms) / 60_000.0, 0.0)
            fair_value = token_fair_value(contract, observation.cex_price, observation.sigma, minutes_to_expiry)
            delta = token_delta(contract, observation.cex_price, observation.sigma, minutes_to_expiry)
            signal_basis = observation.poly_price - fair_value

            history = state.history if state.history is not None else deque()
            zscore = rolling_zscore(history, signal_basis)
            snapshot = SignalSnapshot(
                observation=observation,
                fair_value=fair_value,
                signal_basis=signal_basis,
                zscore=zscore,
                delta=delta,
            )
            signal_snapshots.append(snapshot)

            should_exit = (
                is_evaluation_minute
                and
                state.position_open
                and zscore is not None
                and (abs(zscore) <= cfg.exit_z or zscore >= cfg.entry_z)
            )
            should_enter = (
                is_evaluation_minute
                and
                not state.position_open
                and minutes_to_expiry > MIN_MINUTES_TO_EXPIRY
                and zscore is not None
                and zscore <= -cfg.entry_z
                and observation.poly_price > 0.0
                and observation.cex_price > 0.0
            )

            if is_evaluation_minute:
                all_events.add(contract.event_id)
                all_markets.add(contract.market_id)
                all_tokens.add(contract.token_id)
                market_stat = ensure_market_stat(contract.market_id, contract.event_id)
                market_stat.rows_processed += 1

            if should_exit:
                exit_candidates.append(snapshot)
            elif should_enter:
                entry_candidates.append(snapshot)

        for snapshot in exit_candidates:
            exited_market_ids.add(snapshot.observation.market_id)
            close_position(
                snapshot.observation.token_id,
                current_cex_price,
                market_exit_price=snapshot.observation.poly_price,
                settle_to_payout=False,
            )

        expired_token_ids = [
            token_id
            for token_id, state in token_states.items()
            if state.position_open and contract_specs[token_id].end_ts_ms <= ts_ms
        ]
        for token_id in expired_token_ids:
            exited_market_ids.add(contract_specs[token_id].market_id)
            close_position(token_id, current_cex_price, settle_to_payout=True)

        capital_limit = cfg.initial_capital * max(cfg.max_capital_usage, 0.0)
        ordered_entries = sorted(
            entry_candidates,
            # 和 Rust replay 保持一致：z-score 更极端的优先；同等 z-score 时优先更大的 |basis|。
            key=lambda item: (
                item.zscore if item.zscore is not None else 0.0,
                -abs(item.signal_basis),
            ),
        )
        for snapshot in ordered_entries:
            token_id = snapshot.observation.token_id
            state = token_states[token_id]
            if state.position_open:
                continue

            # 回测默认按“单市场单仓位”计账，避免同一市场同时持有 YES/NO。
            market_has_open_position = any(
                other_state.position_open
                and other_state.market_id == snapshot.observation.market_id
                and other_token_id != token_id
                for other_token_id, other_state in token_states.items()
            )
            if market_has_open_position:
                continue

            # 只有分钟级价格时，不在同一分钟里先平再立刻反手，避免把同一根价格重复当作出入场依据。
            if snapshot.observation.market_id in exited_market_ids:
                continue

            shares, poly_entry_notional, _ = resolve_position_size(cfg, snapshot.observation.poly_price)
            hedge_qty_btc = -shares * snapshot.delta * max(cfg.cex_hedge_ratio, 0.0)
            hedge_entry_notional = abs(hedge_qty_btc) * snapshot.observation.cex_price
            margin_required = hedge_entry_notional * max(cfg.cex_margin_ratio, 0.0)
            reserved_margin_total, capital_in_use = portfolio_usage()
            available_cash = cash - reserved_margin_total
            estimated_entry_fees = (
                poly_entry_notional * (cfg.fee_bps + cfg.slippage_bps) / 10_000.0
                + hedge_entry_notional * (cfg.fee_bps + cfg.slippage_bps) / 10_000.0
            )
            required_cash = poly_entry_notional + margin_required + estimated_entry_fees

            if (
                shares > 0.0
                and poly_entry_notional > 0.0
                and available_cash >= required_cash
                and capital_in_use + poly_entry_notional + margin_required <= capital_limit + 1e-12
            ):
                market_stat = ensure_market_stat(snapshot.observation.market_id, snapshot.observation.event_id)
                cash -= poly_entry_notional
                cash, poly_fee_paid, poly_slippage_paid_trade = apply_trade_costs(
                    cash,
                    poly_entry_notional,
                    cfg.fee_bps,
                    cfg.slippage_bps,
                )
                total_fees += poly_fee_paid
                total_slippage += poly_slippage_paid_trade
                poly_fees_paid += poly_fee_paid
                poly_slippage_paid += poly_slippage_paid_trade
                state.round_trip_pnl -= poly_fee_paid + poly_slippage_paid_trade
                trade_count += 1
                poly_trade_count += 1
                market_stat.trade_count += 1
                market_stat.fees_paid += poly_fee_paid
                market_stat.slippage_paid += poly_slippage_paid_trade

                if hedge_entry_notional > 0.0:
                    cash, cex_fee_paid, cex_slippage_paid_trade = apply_trade_costs(
                        cash,
                        hedge_entry_notional,
                        cfg.fee_bps,
                        cfg.slippage_bps,
                    )
                    total_fees += cex_fee_paid
                    total_slippage += cex_slippage_paid_trade
                    cex_fees_paid += cex_fee_paid
                    cex_slippage_paid += cex_slippage_paid_trade
                    state.round_trip_pnl -= cex_fee_paid + cex_slippage_paid_trade
                    trade_count += 1
                    cex_trade_count += 1
                    market_stat.trade_count += 1
                    market_stat.fees_paid += cex_fee_paid
                    market_stat.slippage_paid += cex_slippage_paid_trade

                state.position_open = True
                state.poly_entry_price = snapshot.observation.poly_price
                state.poly_entry_notional = poly_entry_notional
                state.shares = shares
                state.cex_entry_price = snapshot.observation.cex_price
                state.cex_entry_notional_usd = hedge_entry_notional
                state.cex_qty_btc = hedge_qty_btc
                state.reserved_margin = margin_required

        for snapshot in signal_snapshots:
            state = token_states[snapshot.observation.token_id]
            if state.history is not None:
                state.history.append(snapshot.signal_basis)
            state.last_signal_basis = snapshot.signal_basis
            state.last_price = snapshot.observation.poly_price
            state.last_cex_price = current_cex_price
            state.has_last_price = True

        if not is_evaluation_minute:
            continue

        unrealized = 0.0
        market_value = 0.0
        cex_unrealized_total = 0.0
        gross_exposure = 0.0
        reserved_margin_total = 0.0
        capital_in_use = 0.0
        open_positions = 0
        for token_state in token_states.values():
            if not token_state.position_open or not token_state.has_last_price:
                continue
            open_positions += 1
            current_poly_value = token_state.shares * token_state.last_price
            current_poly_unrealized = current_poly_value - token_state.poly_entry_notional
            current_cex_unrealized = linear_cex_pnl(
                token_state.cex_entry_price,
                token_state.last_cex_price,
                token_state.cex_qty_btc,
            )
            market_value += current_poly_value
            cex_unrealized_total += current_cex_unrealized
            unrealized += current_poly_unrealized + current_cex_unrealized
            gross_exposure += current_poly_value + abs(token_state.cex_qty_btc) * token_state.last_cex_price
            reserved_margin_total += token_state.reserved_margin
            capital_in_use += token_state.poly_entry_notional + token_state.reserved_margin

        max_gross_exposure = max(max_gross_exposure, gross_exposure)
        max_reserved_margin = max(max_reserved_margin, reserved_margin_total)
        max_capital_in_use = max(max_capital_in_use, capital_in_use)
        if abs(cfg.initial_capital) > 1e-12:
            max_capital_utilization = max(max_capital_utilization, capital_in_use / cfg.initial_capital)

        available_cash = cash - reserved_margin_total
        equity = cash + market_value + cex_unrealized_total
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)
        equity_curve.append(
            EquityPoint(
                ts_ms=ts_ms,
                equity=equity,
                cash=cash,
                available_cash=available_cash,
                unrealized_pnl=unrealized,
                gross_exposure=gross_exposure,
                reserved_margin=reserved_margin_total,
                capital_in_use=capital_in_use,
                open_positions=open_positions,
            )
        )

    last_ts = evaluation_minute_keys[-1]
    if equity_curve and equity_curve[-1].open_positions > 0:
        final_cex_price = observations_by_minute[last_ts][0].cex_price
        remaining_token_ids = [token_id for token_id, state in token_states.items() if state.position_open]
        for token_id in remaining_token_ids:
            close_position(token_id, final_cex_price, settle_to_payout=True)
        ending_equity = cash
        equity_curve.append(
            EquityPoint(
                ts_ms=last_ts,
                equity=ending_equity,
                cash=ending_equity,
                available_cash=ending_equity,
                unrealized_pnl=0.0,
                gross_exposure=0.0,
                reserved_margin=0.0,
                capital_in_use=0.0,
                open_positions=0,
            )
        )
    else:
        ending_equity = cash if not equity_curve else equity_curve[-1].equity

    total_pnl = ending_equity - cfg.initial_capital
    win_rate = 0.0
    if round_trip_count > 0:
        win_rate = winning_round_trips / round_trip_count

    returns: list[float] = []
    interval_minutes: list[float] = []
    for idx in range(1, len(equity_curve)):
        previous = equity_curve[idx - 1]
        current = equity_curve[idx]
        if abs(previous.equity) <= 1e-12:
            continue
        returns.append((current.equity - previous.equity) / previous.equity)
        interval_minutes.append(max((current.ts_ms - previous.ts_ms) / 60_000.0, 1.0))

    annualized_sharpe = 0.0
    if len(returns) >= 2:
        avg_return = sum(returns) / len(returns)
        variance = sum((item - avg_return) ** 2 for item in returns) / (len(returns) - 1)
        std_return = math.sqrt(max(variance, 0.0))
        if std_return > 1e-12:
            avg_interval_minutes = sum(interval_minutes) / len(interval_minutes)
            annualization = (365.0 * 24.0 * 60.0) / avg_interval_minutes
            annualized_sharpe = (avg_return / std_return) * math.sqrt(annualization)

    summary = BacktestSummary(
        db_path=cfg.db_path,
        cex_source=metadata.get("cex_source"),
        market_style=metadata.get("market_style"),
        rows_processed=sum(len(observations_by_minute[ts_ms]) for ts_ms in evaluation_minute_keys),
        minute_count=len(evaluation_minute_keys),
        token_count=len(all_tokens),
        market_count=len(all_markets),
        event_count=len(all_events),
        start_ts_ms=evaluation_minute_keys[0],
        end_ts_ms=evaluation_minute_keys[-1],
        initial_capital=cfg.initial_capital,
        ending_equity=ending_equity,
        total_pnl=total_pnl,
        realized_pnl=poly_realized_gross + cex_realized_gross - total_fees - total_slippage,
        unrealized_pnl=0.0,
        poly_realized_pnl=poly_realized_gross - poly_fees_paid - poly_slippage_paid,
        cex_realized_pnl=cex_realized_gross - cex_fees_paid - cex_slippage_paid,
        max_drawdown=max_drawdown,
        max_gross_exposure=max_gross_exposure,
        max_reserved_margin=max_reserved_margin,
        max_capital_in_use=max_capital_in_use,
        max_capital_utilization=max_capital_utilization,
        trade_count=trade_count,
        poly_trade_count=poly_trade_count,
        cex_trade_count=cex_trade_count,
        round_trip_count=round_trip_count,
        winning_round_trips=winning_round_trips,
        win_rate=win_rate,
        total_fees_paid=total_fees,
        total_slippage_paid=total_slippage,
        poly_fees_paid=poly_fees_paid,
        poly_slippage_paid=poly_slippage_paid,
        cex_fees_paid=cex_fees_paid,
        cex_slippage_paid=cex_slippage_paid,
        annualized_sharpe=annualized_sharpe,
        strategy={
            "name": "poly_basis_structural_delta_hedge",
            "rolling_window": cfg.rolling_window,
            "entry_z": cfg.entry_z,
            "exit_z": cfg.exit_z,
            "position_sizing_mode": resolved_position_mode,
            **(
                {"position_units": cfg.position_units}
                if resolved_position_mode == "legacy_poly_units"
                else {"position_notional_usd": resolved_position_notional}
            ),
            "max_capital_usage": cfg.max_capital_usage,
            "cex_hedge_ratio": cfg.cex_hedge_ratio,
            "cex_margin_ratio": cfg.cex_margin_ratio,
            "fee_bps": cfg.fee_bps,
            "slippage_bps": cfg.slippage_bps,
            "signal_basis": "poly_price - causal_terminal_probability",
            "cex_probability_proxy": "lognormal terminal probability from previous completed cex bar and trailing realized volatility",
            "trade_start_ts_ms": cfg.trade_start_ts_ms,
            "settlement_payout_source": "prefer gamma outcomePrices when officially resolved, otherwise fallback to cex-derived terminal payout",
            "pnl_scope": "poly + cex combined",
            "note": "Poly 腿按真实 premium 记账；CEX 腿按结构化概率模型的数值 delta 做方向与规模近似，并统一使用上一根已完成的 CEX bar；这仍是研究回测，不是实盘可成交收益。",
        },
    )

    market_rankings = sorted(
        market_stats.values(),
        key=lambda row: (row.net_pnl(), row.gross_pnl, row.round_trip_count, row.market_id),
        reverse=True,
    )

    return summary, equity_curve, market_rankings


def build_run_config_from_args(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        db_path=args.db_path,
        market_id=args.market_id,
        token_id=args.token_id,
        start_ts_ms=parse_time_to_ms(args.start),
        end_ts_ms=parse_time_to_ms(args.end),
        trade_start_ts_ms=None,
        initial_capital=float(args.initial_capital),
        rolling_window=int(args.rolling_window),
        entry_z=float(args.entry_z),
        exit_z=float(args.exit_z),
        position_units=float(args.position_units) if args.position_units is not None else None,
        position_notional_usd=(
            float(args.position_notional_usd) if args.position_notional_usd is not None else None
        ),
        max_capital_usage=float(args.max_capital_usage),
        cex_hedge_ratio=float(args.cex_hedge_ratio),
        cex_margin_ratio=float(args.cex_margin_ratio),
        fee_bps=float(args.fee_bps),
        slippage_bps=float(args.slippage_bps),
        poly_slippage_bps=float(args.poly_slippage_bps),
        cex_slippage_bps=float(args.cex_slippage_bps),
        min_poly_price=float(args.min_poly_price) if args.min_poly_price is not None else None,
        max_poly_price=float(args.max_poly_price) if args.max_poly_price is not None else None,
        report_json=args.report_json,
        equity_csv=args.equity_csv,
    )


def resolve_position_sizing_config(cfg: RunConfig) -> tuple[str, float | None]:
    resolved_position_mode = "legacy_poly_units" if cfg.position_units is not None else "target_poly_notional_usd"
    resolved_position_notional = cfg.position_notional_usd
    if resolved_position_mode == "target_poly_notional_usd" and resolved_position_notional is None:
        resolved_position_notional = DEFAULT_POSITION_NOTIONAL_USD
    return resolved_position_mode, resolved_position_notional


def compute_annualized_sharpe_from_equity_curve(equity_curve: list[EquityPoint]) -> float:
    returns: list[float] = []
    interval_minutes: list[float] = []
    for idx in range(1, len(equity_curve)):
        previous = equity_curve[idx - 1]
        current = equity_curve[idx]
        if abs(previous.equity) <= 1e-12:
            continue
        returns.append((current.equity - previous.equity) / previous.equity)
        interval_minutes.append(max((current.ts_ms - previous.ts_ms) / 60_000.0, 1.0))

    if len(returns) < 2:
        return 0.0

    avg_return = sum(returns) / len(returns)
    variance = sum((item - avg_return) ** 2 for item in returns) / (len(returns) - 1)
    std_return = math.sqrt(max(variance, 0.0))
    if std_return <= 1e-12:
        return 0.0

    avg_interval_minutes = sum(interval_minutes) / len(interval_minutes)
    annualization = (365.0 * 24.0 * 60.0) / avg_interval_minutes
    return (avg_return / std_return) * math.sqrt(annualization)


def load_equity_curve_csv(path: str) -> list[EquityPoint]:
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        out: list[EquityPoint] = []
        for row in reader:
            out.append(
                EquityPoint(
                    ts_ms=int(row["ts_ms"]),
                    equity=float(row["equity"]),
                    cash=float(row["cash"]),
                    available_cash=float(row["available_cash"]),
                    unrealized_pnl=float(row["unrealized_pnl"]),
                    gross_exposure=float(row["gross_exposure"]),
                    reserved_margin=float(row["reserved_margin"]),
                    capital_in_use=float(row["capital_in_use"]),
                    open_positions=int(float(row["open_positions"])),
                )
            )
    return out


def load_report_context(cfg: RunConfig) -> tuple[dict[str, tuple[str, str]], int]:
    conn = duckdb.connect(cfg.db_path, read_only=True)
    try:
        market_labels = load_market_labels(conn)
        where_sql, params = build_contract_where_clause(cfg)
        token_count_row = conn.execute(
            f"""
            select count(distinct token_id)
            from polymarket_contracts
            {where_sql}
            """,
            params,
        ).fetchone()
    finally:
        conn.close()
    token_count = int(token_count_row[0] or 0) if token_count_row else 0
    return market_labels, token_count


def build_rust_replay_command(
    cfg: RunConfig,
    rust_report_json_path: str,
    rust_equity_csv_path: str,
    rust_anomaly_report_json_path: str,
    *,
    band_policy: str | None = None,
) -> list[str]:
    if cfg.token_id is not None:
        raise SystemExit(
            "`run` 默认走 Rust replay 事实源，当前不支持 `--token-id`；"
            "如需旧版 token 级对照，请改用 `run-legacy`。"
        )
    if cfg.position_units is not None:
        raise SystemExit(
            "`run` 默认走 Rust replay 事实源，当前不支持 `--position-units`；"
            "如需旧版按份数下单口径，请改用 `run-legacy`。"
        )

    _, resolved_position_notional = resolve_position_sizing_config(cfg)
    resolved_band_policy = band_policy or RUNTIME_BACKTEST_DEFAULTS.band_policy
    args = [
        "cargo",
        "run",
        "-q",
        "-p",
        "polyalpha-cli",
        "--",
        "backtest",
        "rust-replay",
        "--db-path",
        cfg.db_path,
        "--initial-capital",
        str(cfg.initial_capital),
        "--rolling-window",
        str(cfg.rolling_window),
        "--entry-z",
        str(cfg.entry_z),
        "--exit-z",
        str(cfg.exit_z),
        "--max-capital-usage",
        str(cfg.max_capital_usage),
        "--cex-hedge-ratio",
        str(cfg.cex_hedge_ratio),
        "--cex-margin-ratio",
        str(cfg.cex_margin_ratio),
        "--poly-fee-bps",
        str(cfg.fee_bps),
        "--poly-slippage-bps",
        str(cfg.poly_slippage_bps),
        "--cex-fee-bps",
        str(cfg.fee_bps),
        "--cex-slippage-bps",
        str(cfg.cex_slippage_bps),
        "--band-policy",
        resolved_band_policy,
        "--report-json",
        rust_report_json_path,
        "--anomaly-report-json",
        rust_anomaly_report_json_path,
        "--fail-on-anomaly",
        "--equity-csv",
        rust_equity_csv_path,
    ]
    if cfg.market_id:
        args.extend(["--market-id", cfg.market_id])
    if cfg.start_ts_ms is not None:
        args.extend(["--start", str(cfg.start_ts_ms)])
    if cfg.end_ts_ms is not None:
        args.extend(["--end", str(cfg.end_ts_ms)])
    if resolved_position_notional is not None:
        args.extend(["--position-notional-usd", str(resolved_position_notional)])
    if cfg.min_poly_price is not None:
        args.extend(["--min-poly-price", str(cfg.min_poly_price)])
    if cfg.max_poly_price is not None:
        args.extend(["--max-poly-price", str(cfg.max_poly_price)])
    return args


def run_rust_replay_fact_source(
    cfg: RunConfig,
    *,
    band_policy: str | None = None,
) -> tuple[BacktestSummary, list[EquityPoint], list[ReportMarketRow], dict[str, Any]]:
    market_labels, token_count = load_report_context(cfg)
    with tempfile.TemporaryDirectory(prefix="polyalpha-rust-replay-") as tempdir:
        rust_report_json_path = os.path.join(tempdir, "rust-report.json")
        rust_equity_csv_path = os.path.join(tempdir, "rust-equity.csv")
        rust_anomaly_report_json_path = os.path.join(tempdir, "rust-anomaly-report.json")
        command = build_rust_replay_command(
            cfg,
            rust_report_json_path,
            rust_equity_csv_path,
            rust_anomaly_report_json_path,
            band_policy=band_policy,
        )
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        rust_report = load_json_if_exists(rust_report_json_path)
        rust_anomaly_report = load_json_if_exists(rust_anomaly_report_json_path)
        if completed.returncode != 0:
            if not is_rust_replay_anomaly_gate_exit(
                completed=completed,
                rust_report=rust_report,
                rust_anomaly_report=rust_anomaly_report,
                rust_equity_csv_path=rust_equity_csv_path,
            ):
                error_text = (completed.stderr or completed.stdout or "").strip()
                raise SystemExit(
                    "Rust replay 执行失败；请先检查 Rust 工程是否可编译。\n"
                    f"命令: {' '.join(command)}\n"
                    f"输出:\n{error_text}"
                )
        if rust_report is None:
            raise SystemExit(f"Rust replay 未产出 report_json: {rust_report_json_path}")
        if rust_anomaly_report is not None:
            rust_report["anomaly_report"] = rust_anomaly_report
        equity_curve = load_equity_curve_csv(rust_equity_csv_path)

    summary = build_report_layer_summary(cfg, rust_report, equity_curve, token_count)
    market_rows = build_report_market_rows(rust_report, market_labels)
    return summary, equity_curve, market_rows, rust_report


def build_report_layer_summary(
    cfg: RunConfig,
    rust_report: dict[str, Any],
    equity_curve: list[EquityPoint],
    token_count: int,
) -> BacktestSummary:
    raw_summary = rust_report["summary"]
    build_metadata = rust_report.get("build_metadata", {})
    anomaly_surface = build_replay_anomaly_surface(rust_report)
    top_anomaly = anomaly_surface.get("top_anomaly")
    _, resolved_position_notional = resolve_position_sizing_config(cfg)
    annualized_sharpe = compute_annualized_sharpe_from_equity_curve(equity_curve)
    poly_realized_pnl = (
        float(raw_summary["poly_realized_gross"])
        - float(raw_summary["poly_fees_paid"])
        - float(raw_summary["poly_slippage_paid"])
    )
    cex_realized_pnl = (
        float(raw_summary["cex_realized_gross"])
        - float(raw_summary["cex_fees_paid"])
        - float(raw_summary["cex_slippage_paid"])
    )
    return BacktestSummary(
        db_path=str(raw_summary["db_path"]),
        cex_source=build_metadata.get("cex_source"),
        market_style=build_metadata.get("market_style"),
        rows_processed=int(raw_summary.get("replayed_poly_observations") or raw_summary.get("poly_observation_count") or 0),
        minute_count=int(raw_summary["minute_count"]),
        token_count=token_count,
        market_count=int(raw_summary["market_count"]),
        event_count=int(raw_summary["event_count"]),
        start_ts_ms=int(raw_summary["start_ts_ms"]) if raw_summary.get("start_ts_ms") is not None else None,
        end_ts_ms=int(raw_summary["end_ts_ms"]) if raw_summary.get("end_ts_ms") is not None else None,
        initial_capital=float(raw_summary["initial_capital"]),
        ending_equity=float(raw_summary["ending_equity"]),
        total_pnl=float(raw_summary["total_pnl"]),
        realized_pnl=float(raw_summary["realized_pnl"]),
        unrealized_pnl=float(raw_summary["unrealized_pnl"]),
        poly_realized_pnl=poly_realized_pnl,
        cex_realized_pnl=cex_realized_pnl,
        max_drawdown=float(raw_summary["max_drawdown"]),
        max_gross_exposure=float(raw_summary["max_gross_exposure"]),
        max_reserved_margin=float(raw_summary["max_reserved_margin"]),
        max_capital_in_use=float(raw_summary["max_capital_in_use"]),
        max_capital_utilization=float(raw_summary["max_capital_utilization"]),
        trade_count=int(raw_summary["trade_count"]),
        poly_trade_count=int(raw_summary["poly_trade_count"]),
        cex_trade_count=int(raw_summary["cex_trade_count"]),
        round_trip_count=int(raw_summary["round_trip_count"]),
        winning_round_trips=int(raw_summary["winning_round_trips"]),
        win_rate=float(raw_summary["win_rate"]),
        total_fees_paid=float(raw_summary["total_fees_paid"]),
        total_slippage_paid=float(raw_summary["total_slippage_paid"]),
        poly_fees_paid=float(raw_summary["poly_fees_paid"]),
        poly_slippage_paid=float(raw_summary["poly_slippage_paid"]),
        cex_fees_paid=float(raw_summary["cex_fees_paid"]),
        cex_slippage_paid=float(raw_summary["cex_slippage_paid"]),
        annualized_sharpe=annualized_sharpe,
        strategy={
            "name": "poly_basis_structural_delta_hedge",
            "execution_source": RUST_REPLAY_FACT_SOURCE,
            "report_layer_role": "python_cli_chinese_report",
            "legacy_reference_mode": "`run-legacy` / `walk-forward`",
            "rolling_window": cfg.rolling_window,
            "entry_z": cfg.entry_z,
            "exit_z": cfg.exit_z,
            "band_policy": raw_summary.get("band_policy", RUNTIME_BACKTEST_DEFAULTS.band_policy),
            "position_sizing_mode": "target_poly_notional_usd",
            "position_notional_usd": resolved_position_notional,
            "max_capital_usage": cfg.max_capital_usage,
            "cex_hedge_ratio": cfg.cex_hedge_ratio,
            "cex_margin_ratio": cfg.cex_margin_ratio,
            "fee_bps": cfg.fee_bps,
            "slippage_bps": cfg.slippage_bps,
            "min_poly_price": cfg.min_poly_price,
            "max_poly_price": cfg.max_poly_price,
            "entry_fill_ratio": float(raw_summary.get("stress", {}).get("entry_fill_ratio", 1.0)),
            "signal_basis": "poly_price - causal_terminal_probability",
            "cex_probability_proxy": "lognormal terminal probability from previous completed cex bar and trailing realized volatility",
            "trade_start_ts_ms": cfg.trade_start_ts_ms,
            "settlement_payout_source": "prefer gamma outcomePrices when officially resolved, otherwise fallback to cex-derived terminal payout",
            "pnl_scope": "poly + cex combined",
            "note": (
                "默认 Python CLI 已降为报表层：实际回放与记账来自 Rust replay；"
                "旧版 Python 回测保留在 `run-legacy` / `walk-forward`，仅供历史对照。"
            ),
        },
        anomaly_count=int(anomaly_surface["anomaly_count"]),
        max_abs_anomaly_equity_jump_usd=float(anomaly_surface["max_abs_anomaly_equity_jump_usd"]),
        max_anomaly_mark_share=float(anomaly_surface["max_anomaly_mark_share"]),
        top_anomaly_start_ts_ms=(
            int(top_anomaly["start_ts_ms"]) if isinstance(top_anomaly, dict) else None
        ),
        top_anomaly_end_ts_ms=(
            int(top_anomaly["end_ts_ms"]) if isinstance(top_anomaly, dict) else None
        ),
        top_anomaly_reason_codes=(
            [str(item) for item in top_anomaly.get("reason_codes", [])]
            if isinstance(top_anomaly, dict)
            else []
        ),
    )


def build_report_market_rows(
    rust_report: dict[str, Any],
    market_labels: dict[str, tuple[str, str]],
) -> list[ReportMarketRow]:
    rows: list[ReportMarketRow] = []
    for item in rust_report["summary"].get("market_debug_rows", []):
        market_id = str(item["market_id"])
        event_id, market_question = market_labels.get(market_id, ("", ""))
        rows.append(
            ReportMarketRow(
                market_id=market_id,
                event_id=event_id,
                market_question=market_question,
                entry_count=int(item.get("entry_count", 0)),
                signal_count=int(item.get("signal_count", 0)),
                rejected_capital_count=int(item.get("rejected_capital_count", 0)),
                net_pnl=float(item.get("net_pnl", 0.0)),
            )
        )
    rows.sort(
        key=lambda row: (row.net_pnl, row.entry_count, row.signal_count, row.market_id),
        reverse=True,
    )
    return rows


def report_market_payload(row: ReportMarketRow) -> dict[str, Any]:
    return {
        "market_id": row.market_id,
        "event_id": row.event_id,
        "market_question": row.market_question,
        "round_trip_count": row.entry_count,
        "entry_count": row.entry_count,
        "signal_count": row.signal_count,
        "rejected_capital_count": row.rejected_capital_count,
        "net_pnl": row.net_pnl,
    }


def report_mode_metadata(report_mode: str, fact_source: str, note: str) -> dict[str, str]:
    return {
        "report_mode": report_mode,
        "fact_source": fact_source,
        "note": note,
    }


def load_json_if_exists(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        loaded = json.load(f)
    if isinstance(loaded, dict):
        return loaded
    return None


def build_replay_anomaly_surface(rust_report: dict[str, Any]) -> dict[str, Any]:
    raw_summary = rust_report.get("summary", {})
    anomaly_report = rust_report.get("anomaly_report", {})
    diagnostics = anomaly_report.get("diagnostics") if isinstance(anomaly_report, dict) else None
    if not isinstance(diagnostics, dict):
        diagnostics = rust_report.get("anomaly_diagnostics", {})
    has_diagnostics = isinstance(diagnostics, dict)
    if not has_diagnostics:
        diagnostics = {}

    def diagnostic_value(key: str, summary_key: str, default: Any) -> Any:
        if key in diagnostics and diagnostics[key] is not None:
            return diagnostics[key]
        if summary_key in raw_summary and raw_summary[summary_key] is not None:
            return raw_summary[summary_key]
        return default

    top_anomalies = diagnostics.get("top_anomalies")
    top_anomaly = top_anomalies[0] if isinstance(top_anomalies, list) and top_anomalies else None
    anomaly_count = int(
        diagnostic_value("anomaly_count", "anomaly_count", 0)
        if has_diagnostics
        else raw_summary.get("anomaly_count", 0)
        or 0
    )
    max_abs_jump = float(
        diagnostic_value("max_abs_equity_jump_usd", "max_abs_anomaly_equity_jump_usd", 0.0)
        if has_diagnostics
        else raw_summary.get("max_abs_anomaly_equity_jump_usd", 0.0)
        or 0.0
    )
    max_mark_share = float(
        diagnostic_value("max_mark_share", "max_anomaly_mark_share", 0.0)
        if has_diagnostics
        else raw_summary.get("max_anomaly_mark_share", 0.0)
        or 0.0
    )

    return {
        "anomaly_count": anomaly_count,
        "max_abs_anomaly_equity_jump_usd": max_abs_jump,
        "max_anomaly_mark_share": max_mark_share,
        "top_anomaly": {
            "start_ts_ms": int(top_anomaly.get("start_ts_ms", 0)),
            "end_ts_ms": int(top_anomaly.get("end_ts_ms", 0)),
            "reason_codes": [str(item) for item in top_anomaly.get("reason_codes", [])],
        }
        if isinstance(top_anomaly, dict)
        else None,
    }


def is_rust_replay_anomaly_gate_exit(
    *,
    completed: subprocess.CompletedProcess[str],
    rust_report: dict[str, Any] | None,
    rust_anomaly_report: dict[str, Any] | None,
    rust_equity_csv_path: str,
) -> bool:
    if completed.returncode != 1:
        return False
    if rust_report is None or rust_anomaly_report is None:
        return False
    if not os.path.exists(rust_equity_csv_path):
        return False
    error_text = f"{completed.stderr or ''}\n{completed.stdout or ''}".lower()
    if "replay anomaly detected" not in error_text:
        return False
    anomaly_count = build_replay_anomaly_surface(
        {"summary": rust_report.get("summary", {}), "anomaly_report": rust_anomaly_report}
    ).get("anomaly_count", 0)
    return int(anomaly_count) > 0


def run_walk_forward(
    cfg: RunConfig,
    train_days: int,
    test_days: int,
    step_days: int,
    max_slices: int,
) -> list[WalkForwardSliceResult]:
    train_ms = max(int(train_days), 0) * 24 * 60 * 60 * 1000
    test_ms = max(int(test_days), 0) * 24 * 60 * 60 * 1000
    step_ms = max(int(step_days), 0) * 24 * 60 * 60 * 1000

    if train_ms <= 0 or test_ms <= 0:
        raise SystemExit("train_days and test_days must be positive")
    if step_ms <= 0:
        step_ms = test_ms

    base_cfg = replace(cfg, report_json=None, equity_csv=None, trade_start_ts_ms=None)
    minute_keys = sorted(prepare_backtest_inputs(base_cfg).observations_by_minute)
    if len(minute_keys) < 2:
        raise SystemExit("not enough aligned observations for walk-forward")

    rolling_results: list[WalkForwardSliceResult] = []
    slice_start_idx = 0
    warmup_ms = max(int(cfg.rolling_window), 2) * 60_000

    while slice_start_idx < len(minute_keys):
        train_start_ts = minute_keys[slice_start_idx]
        train_end_idx = bisect_left(minute_keys, train_start_ts + train_ms, lo=slice_start_idx) - 1
        if train_end_idx < slice_start_idx:
            break

        test_start_idx = train_end_idx + 1
        if test_start_idx >= len(minute_keys):
            break

        test_start_ts = minute_keys[test_start_idx]
        test_end_idx = bisect_left(minute_keys, test_start_ts + test_ms, lo=test_start_idx) - 1
        if test_end_idx < test_start_idx:
            break

        if rolling_results:
            previous = rolling_results[-1]
            if (
                previous.test_start_ts_ms == test_start_ts
                and previous.test_end_ts_ms == minute_keys[test_end_idx]
            ):
                slice_start_idx = test_start_idx
                continue

        train_cfg = replace(
            cfg,
            start_ts_ms=train_start_ts,
            end_ts_ms=minute_keys[train_end_idx],
            trade_start_ts_ms=None,
            report_json=None,
            equity_csv=None,
        )
        test_cfg = replace(
            cfg,
            start_ts_ms=max(train_start_ts, test_start_ts - warmup_ms),
            end_ts_ms=minute_keys[test_end_idx],
            trade_start_ts_ms=test_start_ts,
            report_json=None,
            equity_csv=None,
        )

        train_summary, _, _ = run_backtest(train_cfg)
        test_summary, _, _ = run_backtest(test_cfg)
        rolling_results.append(
            WalkForwardSliceResult(
                slice_index=len(rolling_results) + 1,
                train_start_ts_ms=train_summary.start_ts_ms or train_start_ts,
                train_end_ts_ms=train_summary.end_ts_ms or minute_keys[train_end_idx],
                test_start_ts_ms=test_summary.start_ts_ms or test_start_ts,
                test_end_ts_ms=test_summary.end_ts_ms or minute_keys[test_end_idx],
                train_summary=train_summary,
                test_summary=test_summary,
            )
        )

        if max_slices > 0 and len(rolling_results) >= max_slices:
            break

        next_start_idx = bisect_left(
            minute_keys,
            train_start_ts + step_ms,
            lo=min(slice_start_idx + 1, len(minute_keys) - 1),
        )
        if next_start_idx <= slice_start_idx:
            next_start_idx = slice_start_idx + 1
        slice_start_idx = next_start_idx

    if len(rolling_results) < MIN_WALK_FORWARD_SLICES:
        raise SystemExit("not enough aligned windows for walk-forward")
    return rolling_results


def print_walk_forward_report(
    cfg: RunConfig,
    train_days: int,
    test_days: int,
    step_days: int,
    slices: list[WalkForwardSliceResult],
    *,
    mode_note: str | None = None,
) -> None:
    compounded_equity = cfg.initial_capital
    winning_test_slices = 0
    average_train_return = 0.0
    average_test_return = 0.0

    for row in slices:
        train_return = (
            row.train_summary.total_pnl / row.train_summary.initial_capital
            if abs(row.train_summary.initial_capital) > 1e-12
            else 0.0
        )
        test_return = (
            row.test_summary.total_pnl / row.test_summary.initial_capital
            if abs(row.test_summary.initial_capital) > 1e-12
            else 0.0
        )
        average_train_return += train_return
        average_test_return += test_return
        if row.test_summary.total_pnl > 0.0:
            winning_test_slices += 1
        compounded_equity *= 1.0 + test_return

    average_train_return /= len(slices)
    average_test_return /= len(slices)
    compounded_test_pnl = compounded_equity - cfg.initial_capital

    print_title("BTC 基差策略 Walk-Forward 报告")

    if mode_note:
        print_section("模式说明")
        print_kv("说明", mode_note)

    print_section("窗口配置")
    print_kv("训练窗口", f"{train_days} 天")
    print_kv("测试窗口", f"{test_days} 天")
    print_kv("推进步长", f"{step_days} 天")
    print_kv("切片数量", fmt_int(len(slices)))

    print_section("样本外汇总")
    print_kv("样本外复合期末权益", fmt_money(compounded_equity))
    print_kv("样本外复合净收益", fmt_money(compounded_test_pnl, signed=True))
    print_kv("平均训练收益率", fmt_pct(average_train_return, signed=True))
    print_kv("平均测试收益率", fmt_pct(average_test_return, signed=True))
    print_kv("盈利测试切片占比", fmt_pct(winning_test_slices / len(slices)))

    print_section("逐段结果")
    for row in slices:
        print(
            "  "
            f"{row.slice_index}. train={fmt_period(row.train_start_ts_ms, row.train_end_ts_ms)} "
            f"| test={fmt_period(row.test_start_ts_ms, row.test_end_ts_ms)}"
        )
        print(
            "     "
            f"训练净收益={fmt_money(row.train_summary.total_pnl, signed=True)} "
            f"| 测试净收益={fmt_money(row.test_summary.total_pnl, signed=True)} "
            f"| 测试回撤={fmt_money(row.test_summary.max_drawdown)} "
            f"| 测试往返={fmt_int(row.test_summary.round_trip_count)}"
        )


def dump_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def dump_equity_csv(path: str, equity_curve: list[EquityPoint]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts_ms",
                "ts_utc",
                "equity",
                "cash",
                "available_cash",
                "unrealized_pnl",
                "gross_exposure",
                "reserved_margin",
                "capital_in_use",
                "open_positions",
            ]
        )
        for point in equity_curve:
            ts_utc = datetime.fromtimestamp(point.ts_ms / 1000, tz=UTC).isoformat()
            writer.writerow(
                [
                    point.ts_ms,
                    ts_utc,
                    f"{point.equity:.8f}",
                    f"{point.cash:.8f}",
                    f"{point.available_cash:.8f}",
                    f"{point.unrealized_pnl:.8f}",
                    f"{point.gross_exposure:.8f}",
                    f"{point.reserved_margin:.8f}",
                    f"{point.capital_in_use:.8f}",
                    point.open_positions,
                ]
            )


def build_legacy_report_payload(
    summary: BacktestSummary,
    market_rankings: list[MarketPerformance],
    equity_curve: list[EquityPoint],
) -> dict[str, Any]:
    return {
        "report_metadata": report_mode_metadata(
            report_mode="legacy_python_reference",
            fact_source="legacy_python",
            note=LEGACY_REFERENCE_NOTE,
        ),
        "summary": asdict(summary),
        "market_rankings": [market_payload(row) for row in market_rankings],
        "equity_points": [asdict(point) for point in equity_curve],
    }


def build_report_layer_payload(
    summary: BacktestSummary,
    market_rows: list[ReportMarketRow],
    equity_curve: list[EquityPoint],
    rust_report: dict[str, Any],
) -> dict[str, Any]:
    anomaly_surface = build_replay_anomaly_surface(rust_report)
    return {
        "report_metadata": report_mode_metadata(
            report_mode="rust_fact_source_report_layer",
            fact_source=RUST_REPLAY_FACT_SOURCE,
            note=LEGACY_REFERENCE_NOTE,
        ),
        "acceptance": {
            "status": "pass" if anomaly_surface["anomaly_count"] == 0 else "anomaly_detected",
            "anomaly": anomaly_surface,
        },
        "summary": asdict(summary),
        "market_rankings": [report_market_payload(row) for row in market_rows],
        "equity_points": [asdict(point) for point in equity_curve],
        "rust_replay_report": rust_report,
    }


def print_market_rankings(title: str, rows: list[MarketPerformance]) -> None:
    print_section(title)
    if not rows:
        print_kv("状态", "无可展示市场")
        return

    for idx, row in enumerate(rows, start=1):
        print(
            f"  {idx}. market_id={row.market_id} | 净收益={fmt_money(row.net_pnl(), signed=True)} "
            f"| 往返={fmt_int(row.round_trip_count)} | 胜率={fmt_pct(row.win_rate())}"
        )
        print(f"     问题: {compact_text(row.market_question, max_len=64) or '-'}")


def print_report_market_rankings(title: str, rows: list[ReportMarketRow]) -> None:
    print_section(title)
    if not rows:
        print_kv("状态", "无可展示市场")
        return

    for idx, row in enumerate(rows, start=1):
        print(
            f"  {idx}. market_id={row.market_id} | 净收益={fmt_money(row.net_pnl, signed=True)} "
            f"| 开仓={fmt_int(row.entry_count)} | 信号={fmt_int(row.signal_count)} "
            f"| 资金拒绝={fmt_int(row.rejected_capital_count)}"
        )
        print(f"     问题: {compact_text(row.market_question, max_len=64) or '-'}")


def print_summary(
    summary: BacktestSummary,
    cfg: RunConfig,
    market_rankings: list[MarketPerformance],
    *,
    mode_note: str | None = None,
) -> None:
    total_return = 0.0
    if abs(summary.initial_capital) > 1e-12:
        total_return = summary.total_pnl / summary.initial_capital

    avg_round_trip_pnl = None
    if summary.round_trip_count > 0:
        avg_round_trip_pnl = summary.total_pnl / summary.round_trip_count

    avg_trade_cost = None
    total_cost = summary.total_fees_paid + summary.total_slippage_paid
    if summary.trade_count > 0:
        avg_trade_cost = total_cost / summary.trade_count

    pnl_to_drawdown = None
    if summary.max_drawdown > 1e-12:
        pnl_to_drawdown = summary.total_pnl / summary.max_drawdown

    print_title("BTC 基差策略回测报告")

    if mode_note:
        print_section("模式说明")
        print_kv("说明", mode_note)

    print_section("样本概览")
    print_kv("数据库路径", summary.db_path)
    print_kv("CEX 数据源", fmt_optional_text(summary.cex_source, fallback="-"))
    print_kv("市场样式", fmt_optional_text(summary.market_style, fallback="-"))
    print_kv("样本区间", fmt_period(summary.start_ts_ms, summary.end_ts_ms))
    print_kv("Token-分钟观测数", fmt_int(summary.rows_processed))
    print_kv("唯一分钟点数", fmt_int(summary.minute_count))
    print_kv("覆盖事件数", fmt_int(summary.event_count))
    print_kv("覆盖市场数", fmt_int(summary.market_count))
    print_kv("覆盖 token 数", fmt_int(summary.token_count))

    print_section("筛选条件")
    print_kv("market_id", fmt_optional_text(cfg.market_id))
    print_kv("token_id", fmt_optional_text(cfg.token_id))
    print_kv("开始时间", fmt_filter_time(cfg.start_ts_ms))
    print_kv("结束时间", fmt_filter_time(cfg.end_ts_ms))

    print_section("核心业绩")
    print_kv("初始资金", fmt_money(summary.initial_capital))
    print_kv("期末权益", fmt_money(summary.ending_equity))
    print_kv("组合净收益", fmt_money(summary.total_pnl, signed=True))
    print_kv("总收益率", fmt_pct(total_return, signed=True))
    print_kv("Poly 腿净收益", fmt_money(summary.poly_realized_pnl, signed=True))
    print_kv("CEX 腿净收益", fmt_money(summary.cex_realized_pnl, signed=True))
    print_kv("已实现收益", fmt_money(summary.realized_pnl, signed=True))
    print_kv("未实现收益", fmt_money(summary.unrealized_pnl, signed=True))
    print_kv("年化夏普", fmt_ratio(summary.annualized_sharpe))
    print_kv("最大回撤", fmt_money(summary.max_drawdown))
    print_kv("组合名义敞口峰值", fmt_money(summary.max_gross_exposure))
    print_kv("峰值保证金占用", fmt_money(summary.max_reserved_margin))
    print_kv("峰值资金占用", fmt_money(summary.max_capital_in_use))
    print_kv("峰值资金使用率", fmt_pct(summary.max_capital_utilization))
    print_kv("收益回撤比", fmt_ratio(pnl_to_drawdown) if pnl_to_drawdown is not None else "-")

    print_section("交易统计")
    print_kv("组合成交笔数", fmt_int(summary.trade_count))
    print_kv("Poly 腿成交笔数", fmt_int(summary.poly_trade_count))
    print_kv("CEX 腿成交笔数", fmt_int(summary.cex_trade_count))
    print_kv("往返次数", fmt_int(summary.round_trip_count))
    print_kv("盈利往返次数", fmt_int(summary.winning_round_trips))
    print_kv("胜率", fmt_pct(summary.win_rate))
    print_kv(
        "单次往返平均收益",
        fmt_money(avg_round_trip_pnl, signed=True) if avg_round_trip_pnl is not None else "-",
    )

    print_section("成本拆解")
    print_kv("Poly 手续费", fmt_money(summary.poly_fees_paid))
    print_kv("Poly 滑点", fmt_money(summary.poly_slippage_paid))
    print_kv("CEX 手续费", fmt_money(summary.cex_fees_paid))
    print_kv("CEX 滑点", fmt_money(summary.cex_slippage_paid))
    print_kv("总手续费", fmt_money(summary.total_fees_paid))
    print_kv("总滑点", fmt_money(summary.total_slippage_paid))
    print_kv("总成本", fmt_money(total_cost))
    print_kv("单笔平均成本", fmt_money(avg_trade_cost) if avg_trade_cost is not None else "-")

    print_section("策略参数")
    for key, value in summary.strategy.items():
        print_kv(strategy_label(key), strategy_value_display(key, value))

    if len(market_rankings) > 1:
        if len(market_rankings) <= 10:
            print_market_rankings("单市场净收益排行", market_rankings)
        else:
            top_rows = market_rankings[:5]
            bottom_rows = list(reversed(sorted(market_rankings, key=lambda row: row.net_pnl())[:5]))
            print_market_rankings("单市场净收益前 5", top_rows)
            print_market_rankings("单市场净收益后 5", bottom_rows)


def print_report_layer_summary(
    summary: BacktestSummary,
    cfg: RunConfig,
    market_rows: list[ReportMarketRow],
    rust_report: dict[str, Any],
) -> None:
    raw_summary = rust_report["summary"]
    anomaly_surface = build_replay_anomaly_surface(rust_report)
    top_anomaly = anomaly_surface.get("top_anomaly")
    total_return = 0.0
    if abs(summary.initial_capital) > 1e-12:
        total_return = summary.total_pnl / summary.initial_capital

    avg_round_trip_pnl = None
    if summary.round_trip_count > 0:
        avg_round_trip_pnl = summary.total_pnl / summary.round_trip_count

    avg_trade_cost = None
    total_cost = summary.total_fees_paid + summary.total_slippage_paid
    if summary.trade_count > 0:
        avg_trade_cost = total_cost / summary.trade_count

    pnl_to_drawdown = None
    if summary.max_drawdown > 1e-12:
        pnl_to_drawdown = summary.total_pnl / summary.max_drawdown

    print_title("BTC 基差策略回测报告")

    print_section("模式说明")
    print_kv("默认事实源", "Rust replay")
    print_kv("Python 层职责", "中文报表与导出")
    print_kv("历史对照入口", "`run-legacy` / `walk-forward`")
    print_kv("备注", LEGACY_REFERENCE_NOTE)

    print_section("样本概览")
    print_kv("数据库路径", summary.db_path)
    print_kv("CEX 数据源", fmt_optional_text(summary.cex_source, fallback="-"))
    print_kv("市场样式", fmt_optional_text(summary.market_style, fallback="-"))
    print_kv("样本区间", fmt_period(summary.start_ts_ms, summary.end_ts_ms))
    print_kv("Token-分钟观测数", fmt_int(summary.rows_processed))
    print_kv("唯一分钟点数", fmt_int(summary.minute_count))
    print_kv("覆盖事件数", fmt_int(summary.event_count))
    print_kv("覆盖市场数", fmt_int(summary.market_count))
    print_kv("覆盖 token 数", fmt_int(summary.token_count))

    print_section("筛选条件")
    print_kv("market_id", fmt_optional_text(cfg.market_id))
    print_kv("token_id", fmt_optional_text(cfg.token_id))
    print_kv("开始时间", fmt_filter_time(cfg.start_ts_ms))
    print_kv("结束时间", fmt_filter_time(cfg.end_ts_ms))

    print_section("核心业绩")
    print_kv("初始资金", fmt_money(summary.initial_capital))
    print_kv("期末权益", fmt_money(summary.ending_equity))
    print_kv("组合净收益", fmt_money(summary.total_pnl, signed=True))
    print_kv("总收益率", fmt_pct(total_return, signed=True))
    print_kv("Poly 腿净收益", fmt_money(summary.poly_realized_pnl, signed=True))
    print_kv("CEX 腿净收益", fmt_money(summary.cex_realized_pnl, signed=True))
    print_kv("已实现收益", fmt_money(summary.realized_pnl, signed=True))
    print_kv("未实现收益", fmt_money(summary.unrealized_pnl, signed=True))
    print_kv("年化夏普", fmt_ratio(summary.annualized_sharpe))
    print_kv("最大回撤", fmt_money(summary.max_drawdown))
    print_kv("组合名义敞口峰值", fmt_money(summary.max_gross_exposure))
    print_kv("峰值保证金占用", fmt_money(summary.max_reserved_margin))
    print_kv("峰值资金占用", fmt_money(summary.max_capital_in_use))
    print_kv("峰值资金使用率", fmt_pct(summary.max_capital_utilization))
    print_kv("收益回撤比", fmt_ratio(pnl_to_drawdown) if pnl_to_drawdown is not None else "-")

    print_section("交易统计")
    print_kv("组合成交笔数", fmt_int(summary.trade_count))
    print_kv("Poly 腿成交笔数", fmt_int(summary.poly_trade_count))
    print_kv("CEX 腿成交笔数", fmt_int(summary.cex_trade_count))
    print_kv("往返次数", fmt_int(summary.round_trip_count))
    print_kv("盈利往返次数", fmt_int(summary.winning_round_trips))
    print_kv("胜率", fmt_pct(summary.win_rate))
    print_kv(
        "单次往返平均收益",
        fmt_money(avg_round_trip_pnl, signed=True) if avg_round_trip_pnl is not None else "-",
    )

    print_section("Rust 回放统计")
    print_kv("信号数", fmt_int(int(raw_summary.get("signal_count", 0))))
    print_kv("资金拒绝信号数", fmt_int(int(raw_summary.get("signal_rejected_capital", 0))))
    print_kv("缺价拒绝信号数", fmt_int(int(raw_summary.get("signal_rejected_missing_price", 0))))
    print_kv("已有持仓忽略数", fmt_int(int(raw_summary.get("signal_ignored_existing_position", 0))))
    print_kv("主动平仓次数", fmt_int(int(raw_summary.get("close_count", 0))))
    print_kv("到期结算次数", fmt_int(int(raw_summary.get("settlement_count", 0))))

    print_section("异常检测")
    print_kv("验收状态", "通过" if anomaly_surface["anomaly_count"] == 0 else "触发异常门限")
    print_kv("异常窗口数", fmt_int(int(anomaly_surface["anomaly_count"])))
    print_kv(
        "最大异常权益跳变",
        fmt_money(float(anomaly_surface["max_abs_anomaly_equity_jump_usd"]), signed=True),
    )
    print_kv("最大异常 mark 占比", fmt_pct(float(anomaly_surface["max_anomaly_mark_share"])))
    if isinstance(top_anomaly, dict):
        print_kv(
            "Top 异常窗口",
            f"{fmt_ts_ms(int(top_anomaly['start_ts_ms']))} -> {fmt_ts_ms(int(top_anomaly['end_ts_ms']))}",
        )
        print_kv("Top 异常原因", ",".join(top_anomaly.get("reason_codes", [])) or "-")
    else:
        print_kv("Top 异常窗口", "-")
        print_kv("Top 异常原因", "-")

    print_section("成本拆解")
    print_kv("Poly 手续费", fmt_money(summary.poly_fees_paid))
    print_kv("Poly 滑点", fmt_money(summary.poly_slippage_paid))
    print_kv("CEX 手续费", fmt_money(summary.cex_fees_paid))
    print_kv("CEX 滑点", fmt_money(summary.cex_slippage_paid))
    print_kv("总手续费", fmt_money(summary.total_fees_paid))
    print_kv("总滑点", fmt_money(summary.total_slippage_paid))
    print_kv("总成本", fmt_money(total_cost))
    print_kv("单笔平均成本", fmt_money(avg_trade_cost) if avg_trade_cost is not None else "-")

    print_section("策略参数")
    for key, value in summary.strategy.items():
        print_kv(strategy_label(key), strategy_value_display(key, value))

    if len(market_rows) > 1:
        top_rows = market_rows[:5]
        bottom_rows = list(reversed(sorted(market_rows, key=lambda row: row.net_pnl)[:5]))
        print_report_market_rankings("单市场净收益前 5", top_rows)
        print_report_market_rankings("单市场净收益后 5", bottom_rows)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "inspect-db":
        inspect_db(args.db_path, args.show_failures)
        return

    if args.command in {"run", "run-current-noband"}:
        cfg = build_run_config_from_args(args)
        band_policy = "disabled" if args.command == "run-current-noband" else None
        summary, equity_curve, market_rows, rust_report = run_rust_replay_fact_source(
            cfg,
            band_policy=band_policy,
        )
        print_report_layer_summary(summary, cfg, market_rows, rust_report)

        if cfg.report_json:
            dump_json(cfg.report_json, build_report_layer_payload(summary, market_rows, equity_curve, rust_report))
            print()
            print_kv("已写出 report_json", cfg.report_json)

        if cfg.equity_csv:
            dump_equity_csv(cfg.equity_csv, equity_curve)
            print_kv("已写出 equity_csv", cfg.equity_csv)
        if summary.anomaly_count > 0:
            raise SystemExit(1)
        return

    if args.command == "run-legacy":
        cfg = build_run_config_from_args(args)
        summary, equity_curve, market_rankings = run_backtest(cfg)
        print_summary(summary, cfg, market_rankings, mode_note=LEGACY_REFERENCE_NOTE)

        if cfg.report_json:
            dump_json(cfg.report_json, build_legacy_report_payload(summary, market_rankings, equity_curve))
            print()
            print_kv("已写出 report_json", cfg.report_json)

        if cfg.equity_csv:
            dump_equity_csv(cfg.equity_csv, equity_curve)
            print_kv("已写出 equity_csv", cfg.equity_csv)
        return

    if args.command == "walk-forward":
        cfg = build_run_config_from_args(args)
        slices = run_walk_forward(
            cfg,
            train_days=int(args.train_days),
            test_days=int(args.test_days),
            step_days=int(args.step_days),
            max_slices=int(args.max_slices),
        )
        print_walk_forward_report(
            cfg,
            train_days=int(args.train_days),
            test_days=int(args.test_days),
            step_days=int(args.step_days),
            slices=slices,
            mode_note=LEGACY_REFERENCE_NOTE,
        )

        if cfg.report_json:
            payload = {
                "report_metadata": report_mode_metadata(
                    report_mode="legacy_python_walk_forward",
                    fact_source="legacy_python",
                    note=LEGACY_REFERENCE_NOTE,
                ),
                "walk_forward": {
                    "train_days": int(args.train_days),
                    "test_days": int(args.test_days),
                    "step_days": int(args.step_days),
                    "max_slices": int(args.max_slices),
                    "slices": [asdict(item) for item in slices],
                }
            }
            dump_json(cfg.report_json, payload)
            print()
            print_kv("已写出 report_json", cfg.report_json)

        if cfg.equity_csv:
            print_kv("equity_csv", "walk-forward 当前不导出聚合权益曲线，已忽略该参数")
        return

    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
