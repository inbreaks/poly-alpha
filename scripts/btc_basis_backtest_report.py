#!/usr/bin/env python3
"""
BTC 基差回测与报告脚本。

子命令:
1) inspect-db: 检查 DuckDB 元数据、时间范围和行数
2) run: 运行最小可交付策略并输出业绩报告

依赖:
    python3 -m pip install -r requirements/backtest-db.txt
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import deque
from dataclasses import asdict, dataclass
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


DEFAULT_DB_PATH = "data/btc_basis_backtest_price_only_ready.duckdb"
DEFAULT_INITIAL_CAPITAL = 100_000.0
# 基于已清洗的近三个月 BTC 样本做过一轮聚焦搜索，默认先采用当前最优组合。
DEFAULT_WINDOW = 180
DEFAULT_ENTRY_Z = 1.5
DEFAULT_EXIT_Z = 0.75
DEFAULT_POSITION_NOTIONAL_PCT = 0.01
DEFAULT_MAX_CAPITAL_USAGE = 0.25
DEFAULT_CEX_HEDGE_RATIO = 1.0
DEFAULT_CEX_MARGIN_RATIO = 0.10
DEFAULT_FEE_BPS = 1.0
DEFAULT_SLIPPAGE_BPS = 2.0
LINE_WIDTH = 78


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
    cex_notional_usd: float = 0.0
    reserved_margin: float = 0.0
    last_cex_price: float = 0.0
    last_signal_basis: float = 0.0
    has_last_price: bool = False
    history: deque[float] | None = None
    round_trip_pnl: float = 0.0


@dataclass
class RunConfig:
    db_path: str
    market_id: str | None
    token_id: str | None
    start_ts_ms: int | None
    end_ts_ms: int | None
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
    report_json: str | None
    equity_csv: str | None


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
        "cex_source": "CEX 数据源",
        "discovery_source": "市场发现源",
        "market_style": "市场样式",
    }
    return labels.get(key, key)


def table_label(name: str) -> str:
    labels = {
        "gamma_events": "Gamma 事件表",
        "polymarket_contracts": "Polymarket 合约表",
        "polymarket_price_history": "Polymarket 历史价格表",
        "polymarket_history_failures": "Polymarket 抓取失败表",
        "binance_btc_1m": "Binance BTC 1m K线表",
        "basis_1m": "分钟级基差表",
    }
    return labels.get(name, name)


def strategy_label(key: str) -> str:
    labels = {
        "name": "策略名称",
        "rolling_window": "滚动窗口",
        "entry_z": "入场阈值",
        "exit_z": "出场阈值",
        "position_sizing_mode": "仓位模式",
        "position_units": "单次 Poly 份数",
        "position_notional_usd": "单次 Poly 名义",
        "max_capital_usage": "最大资金使用率",
        "cex_hedge_ratio": "CEX 名义对冲比",
        "cex_margin_ratio": "CEX 初始保证金率",
        "fee_bps": "单腿手续费",
        "slippage_bps": "单腿滑点",
        "signal_basis": "信号基差",
        "cex_probability_proxy": "CEX 概率代理",
        "pnl_scope": "收益统计范围",
        "note": "口径说明",
    }
    return labels.get(key, key)


def strategy_value_display(key: str, value: Any) -> str:
    if key == "rolling_window":
        return f"{value} 分钟"
    if key in {"entry_z", "exit_z"}:
        return fmt_ratio(float(value))
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
    if key == "name" and value == "poly_basis_with_linear_cex_hedge":
        return "Poly + CEX 组合均值回归"
    if key == "signal_basis" and value == "poly_price - cex_probability_proxy":
        return "Poly 价格 - CEX 概率代理"
    if (
        key == "cex_probability_proxy"
        and value == "per-market min-max normalization of cex_price, inverted for NO tokens"
    ):
        return "按单市场对 CEX 价格做 min-max 归一化，NO token 取反"
    if key == "pnl_scope" and value == "poly + cex combined":
        return "统计 Poly + CEX 两腿组合收益"
    if (
        key == "note"
        and value
        == "Poly 腿按真实 premium 记账；CEX 腿按 BTC 线性永续的名义对冲近似记账，并计入保证金占用与两腿成本；这仍不是严格 Delta 对冲。"
    ):
        return "Poly 腿按真实 premium 记账；CEX 腿按 BTC 线性永续名义对冲近似记账，并计入保证金占用与两腿成本；这仍不是严格 Delta 对冲。"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BTC 基差回测报表 CLI")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="DuckDB 数据库路径")

    sub = parser.add_subparsers(dest="command", required=True)

    inspect_parser = sub.add_parser("inspect-db", help="检查数据库元数据与样本覆盖情况")
    inspect_parser.add_argument("--show-failures", action="store_true", help="显示抓取失败样例")

    run_parser = sub.add_parser("run", help="运行最小可交付基差策略回测")
    run_parser.add_argument("--market-id", default=None, help="可选 market_id 过滤")
    run_parser.add_argument("--token-id", default=None, help="可选 token_id 过滤")
    run_parser.add_argument("--start", default=None, help="开始时间: ts_ms / YYYY-MM-DD / ISO8601")
    run_parser.add_argument("--end", default=None, help="结束时间: ts_ms / YYYY-MM-DD / ISO8601")
    run_parser.add_argument(
        "--initial-capital",
        type=float,
        default=DEFAULT_INITIAL_CAPITAL,
        help="初始资金，单位 USD",
    )
    run_parser.add_argument(
        "--rolling-window",
        type=int,
        default=DEFAULT_WINDOW,
        help="滚动窗口，单位分钟",
    )
    run_parser.add_argument("--entry-z", type=float, default=DEFAULT_ENTRY_Z, help="入场 z-score")
    run_parser.add_argument("--exit-z", type=float, default=DEFAULT_EXIT_Z, help="出场 z-score")
    run_parser.add_argument(
        "--position-units",
        type=float,
        default=None,
        help="兼容模式：每次信号买入的 Poly 份数；不传时改用按美元名义下单",
    )
    run_parser.add_argument(
        "--position-notional-usd",
        type=float,
        default=None,
        help="每次信号买入的 Poly 名义金额；默认按初始资金的一定比例自动推导",
    )
    run_parser.add_argument(
        "--max-capital-usage",
        type=float,
        default=DEFAULT_MAX_CAPITAL_USAGE,
        help="允许占用的最大资金比例，按 Poly premium + CEX 保证金计算",
    )
    run_parser.add_argument(
        "--cex-hedge-ratio",
        type=float,
        default=DEFAULT_CEX_HEDGE_RATIO,
        help="CEX 对冲名义 / Poly 名义",
    )
    run_parser.add_argument(
        "--cex-margin-ratio",
        type=float,
        default=DEFAULT_CEX_MARGIN_RATIO,
        help="CEX 线性永续初始保证金率",
    )
    run_parser.add_argument("--fee-bps", type=float, default=DEFAULT_FEE_BPS, help="单边手续费 bps")
    run_parser.add_argument(
        "--slippage-bps",
        type=float,
        default=DEFAULT_SLIPPAGE_BPS,
        help="单边滑点 bps",
    )
    run_parser.add_argument("--report-json", default=None, help="可选 JSON 报告输出路径")
    run_parser.add_argument("--equity-csv", default=None, help="可选权益曲线 CSV 输出路径")

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
        table_names = [
            "gamma_events",
            "polymarket_contracts",
            "polymarket_price_history",
            "polymarket_history_failures",
            "binance_btc_1m",
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

        print_title("BTC 基差回测数据库检查")

        print_section("数据库")
        print_kv("数据库路径", db_path)
        if metadata:
            print_section("构建元数据")
            for key in sorted(metadata.keys()):
                print_kv(metadata_label(key), metadata[key])
        else:
            print_section("构建元数据")
            print_kv("状态", "空")

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


def load_basis_rows(conn: Any, cfg: RunConfig) -> list[tuple[int, str, str, str, str, float, float]]:
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

    sql = f"""
        select
            ts_ms,
            event_id,
            market_id,
            token_id,
            token_side,
            poly_price,
            cex_price
        from basis_1m
        {where_sql}
        order by ts_ms asc, token_id asc
    """
    return conn.execute(sql, params).fetchall()


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
        target_notional = cfg.initial_capital * DEFAULT_POSITION_NOTIONAL_PCT
    target_notional = max(target_notional, 0.0)
    shares = target_notional / poly_price
    return shares, target_notional, "target_poly_notional_usd"


def linear_cex_pnl(entry_price: float, exit_price: float, notional_usd: float) -> float:
    if entry_price <= 1e-12 or notional_usd <= 0.0:
        return 0.0
    # 回测里把 CEX 腿近似成名义金额固定的 BTC 线性空头，用于组合收益与资金占用估算。
    return notional_usd * (entry_price - exit_price) / entry_price


def build_market_cex_bounds(rows: list[tuple[int, str, str, str, str, float, float]]) -> dict[str, tuple[float, float]]:
    bounds: dict[str, tuple[float, float]] = {}
    for _, _, market_id, _, _, _, cex_price in rows:
        current = bounds.get(market_id)
        if current is None:
            bounds[market_id] = (cex_price, cex_price)
            continue
        bounds[market_id] = (min(current[0], cex_price), max(current[1], cex_price))
    return bounds


def cex_probability_proxy(token_side: str, cex_price: float, cex_bounds: tuple[float, float]) -> float:
    lower, upper = cex_bounds
    if upper <= lower + 1e-12:
        normalized = 0.5
    else:
        normalized = (cex_price - lower) / (upper - lower)
        normalized = min(1.0, max(0.0, normalized))

    if token_side.lower() == "no":
        return 1.0 - normalized
    return normalized


def run_backtest(cfg: RunConfig) -> tuple[BacktestSummary, list[EquityPoint], list[MarketPerformance]]:
    conn = duckdb.connect(cfg.db_path, read_only=True)
    try:
        metadata = fetch_metadata(conn)
        market_labels = load_market_labels(conn)
        rows = load_basis_rows(conn, cfg)
    finally:
        conn.close()

    if not rows:
        raise SystemExit("no basis rows matched filter; try inspect-db first")

    market_cex_bounds = build_market_cex_bounds(rows)
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
        resolved_position_notional = cfg.initial_capital * DEFAULT_POSITION_NOTIONAL_PCT

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

    for ts_ms, event_id, market_id, token_id, token_side, poly_price, cex_price in rows:
        all_events.add(event_id)
        all_markets.add(market_id)
        all_tokens.add(token_id)
        market_stat = ensure_market_stat(market_id, event_id)
        market_stat.rows_processed += 1

        state = token_states.get(token_id)
        if state is None:
            state = TokenState(
                event_id=event_id,
                market_id=market_id,
                history=deque(maxlen=cfg.rolling_window),
            )
            token_states[token_id] = state

        cex_proxy = cex_probability_proxy(token_side, cex_price, market_cex_bounds[market_id])
        signal_basis = poly_price - cex_proxy

        # 使用当前 bar 之前的数据计算 z-score，避免看未来。
        history = state.history if state.history is not None else deque()
        z = rolling_zscore(history, signal_basis)

        should_enter = (
            not state.position_open
            and z is not None
            and z <= -cfg.entry_z
            and poly_price > 0.0
            and cex_price > 0.0
        )
        should_exit = (
            state.position_open
            and z is not None
            and (abs(z) <= cfg.exit_z or z >= cfg.entry_z)
        )

        if should_exit:
            sale_value = state.shares * poly_price
            poly_gross_pnl = state.shares * (poly_price - state.poly_entry_price)
            cash += sale_value
            poly_realized_gross += poly_gross_pnl
            state.round_trip_pnl += poly_gross_pnl
            market_stat.gross_pnl += poly_gross_pnl

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

            cex_gross_pnl = linear_cex_pnl(state.cex_entry_price, cex_price, state.cex_notional_usd)
            cash += cex_gross_pnl
            cex_realized_gross += cex_gross_pnl
            state.round_trip_pnl += cex_gross_pnl
            market_stat.gross_pnl += cex_gross_pnl
            if state.cex_notional_usd > 0.0:
                cash, cex_fee_paid, cex_slippage_paid_trade = apply_trade_costs(
                    cash,
                    state.cex_notional_usd,
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
            if state.round_trip_pnl > 0:
                winning_round_trips += 1
                market_stat.winning_round_trips += 1
            state.round_trip_pnl = 0.0
            state.position_open = False
            state.poly_entry_price = 0.0
            state.poly_entry_notional = 0.0
            state.shares = 0.0
            state.cex_entry_price = 0.0
            state.cex_notional_usd = 0.0
            state.reserved_margin = 0.0

        if should_enter:
            shares, poly_entry_notional, _ = resolve_position_size(cfg, poly_price)
            hedge_notional_usd = poly_entry_notional * max(cfg.cex_hedge_ratio, 0.0)
            reserved_margin_total = sum(
                token_state.reserved_margin for token_state in token_states.values() if token_state.position_open
            )
            capital_in_use = sum(
                token_state.poly_entry_notional + token_state.reserved_margin
                for token_state in token_states.values()
                if token_state.position_open
            )
            margin_required = hedge_notional_usd * max(cfg.cex_margin_ratio, 0.0)
            # `cash` 里包含了已预留给 CEX 的保证金，因此开仓检查要看可用现金而不是总现金。
            available_cash = cash - reserved_margin_total
            estimated_entry_fees = (
                poly_entry_notional * (cfg.fee_bps + cfg.slippage_bps) / 10_000.0
                + hedge_notional_usd * (cfg.fee_bps + cfg.slippage_bps) / 10_000.0
            )
            required_cash = poly_entry_notional + margin_required + estimated_entry_fees
            capital_limit = cfg.initial_capital * max(cfg.max_capital_usage, 0.0)

            if (
                shares > 0.0
                and poly_entry_notional > 0.0
                and available_cash >= required_cash
                and capital_in_use + poly_entry_notional + margin_required <= capital_limit + 1e-12
            ):
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

                if hedge_notional_usd > 0.0:
                    cash, cex_fee_paid, cex_slippage_paid_trade = apply_trade_costs(
                        cash,
                        hedge_notional_usd,
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
                state.poly_entry_price = poly_price
                state.poly_entry_notional = poly_entry_notional
                state.shares = shares
                state.cex_entry_price = cex_price
                state.cex_notional_usd = hedge_notional_usd
                state.reserved_margin = margin_required

        if state.history is not None:
            state.history.append(signal_basis)

        state.last_signal_basis = signal_basis
        state.last_price = poly_price
        state.last_cex_price = cex_price
        state.has_last_price = True

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
                token_state.cex_notional_usd,
            )
            market_value += current_poly_value
            cex_unrealized_total += current_cex_unrealized
            unrealized += current_poly_unrealized + current_cex_unrealized
            gross_exposure += current_poly_value + token_state.cex_notional_usd
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
        dd = peak_equity - equity
        if dd > max_drawdown:
            max_drawdown = dd

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

    if rows:
        last_ts = rows[-1][0]
        for token_state in token_states.values():
            if not token_state.position_open:
                continue
            market_stat = ensure_market_stat(token_state.market_id, token_state.event_id)
            sale_value = token_state.shares * token_state.last_price
            poly_gross_pnl = token_state.shares * (token_state.last_price - token_state.poly_entry_price)
            cash += sale_value
            poly_realized_gross += poly_gross_pnl
            token_state.round_trip_pnl += poly_gross_pnl
            market_stat.gross_pnl += poly_gross_pnl
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
            token_state.round_trip_pnl -= poly_fee_paid + poly_slippage_paid_trade
            trade_count += 1
            poly_trade_count += 1
            market_stat.trade_count += 1
            market_stat.fees_paid += poly_fee_paid
            market_stat.slippage_paid += poly_slippage_paid_trade

            cex_gross_pnl = linear_cex_pnl(
                token_state.cex_entry_price,
                token_state.last_cex_price,
                token_state.cex_notional_usd,
            )
            cash += cex_gross_pnl
            cex_realized_gross += cex_gross_pnl
            token_state.round_trip_pnl += cex_gross_pnl
            market_stat.gross_pnl += cex_gross_pnl
            if token_state.cex_notional_usd > 0.0:
                cash, cex_fee_paid, cex_slippage_paid_trade = apply_trade_costs(
                    cash,
                    token_state.cex_notional_usd,
                    cfg.fee_bps,
                    cfg.slippage_bps,
                )
                total_fees += cex_fee_paid
                total_slippage += cex_slippage_paid_trade
                cex_fees_paid += cex_fee_paid
                cex_slippage_paid += cex_slippage_paid_trade
                token_state.round_trip_pnl -= cex_fee_paid + cex_slippage_paid_trade
                trade_count += 1
                cex_trade_count += 1
                market_stat.trade_count += 1
                market_stat.fees_paid += cex_fee_paid
                market_stat.slippage_paid += cex_slippage_paid_trade

            round_trip_count += 1
            market_stat.round_trip_count += 1
            if token_state.round_trip_pnl > 0:
                winning_round_trips += 1
                market_stat.winning_round_trips += 1
            token_state.round_trip_pnl = 0.0
            token_state.position_open = False
            token_state.shares = 0.0
            token_state.poly_entry_price = 0.0
            token_state.poly_entry_notional = 0.0
            token_state.cex_entry_price = 0.0
            token_state.cex_notional_usd = 0.0
            token_state.reserved_margin = 0.0

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
        ending_equity = cash

    total_pnl = ending_equity - cfg.initial_capital
    win_rate = 0.0
    if round_trip_count > 0:
        win_rate = winning_round_trips / round_trip_count

    returns = []
    for i in range(1, len(equity_curve)):
        prev = equity_curve[i - 1].equity
        curr = equity_curve[i].equity
        if abs(prev) > 1e-12:
            returns.append((curr - prev) / prev)
    annualized_sharpe = 0.0
    if len(returns) >= 2:
        avg = sum(returns) / len(returns)
        var = sum((r - avg) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(max(var, 0.0))
        if std > 1e-12:
            annualized_sharpe = (avg / std) * math.sqrt(365.0 * 24.0 * 60.0)

    summary = BacktestSummary(
        db_path=cfg.db_path,
        cex_source=metadata.get("cex_source"),
        market_style=metadata.get("market_style"),
        rows_processed=len(rows),
        token_count=len(all_tokens),
        market_count=len(all_markets),
        event_count=len(all_events),
        start_ts_ms=rows[0][0] if rows else None,
        end_ts_ms=rows[-1][0] if rows else None,
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
            "name": "poly_basis_with_linear_cex_hedge",
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
            "signal_basis": "poly_price - cex_probability_proxy",
            "cex_probability_proxy": "per-market min-max normalization of cex_price, inverted for NO tokens",
            "pnl_scope": "poly + cex combined",
            "note": "Poly 腿按真实 premium 记账；CEX 腿按 BTC 线性永续的名义对冲近似记账，并计入保证金占用与两腿成本；这仍不是严格 Delta 对冲。",
        },
    )

    market_rankings = sorted(
        market_stats.values(),
        key=lambda row: (row.net_pnl(), row.gross_pnl, row.round_trip_count, row.market_id),
        reverse=True,
    )

    return summary, equity_curve, market_rankings


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


def print_summary(
    summary: BacktestSummary,
    cfg: RunConfig,
    market_rankings: list[MarketPerformance],
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

    print_section("样本概览")
    print_kv("数据库路径", summary.db_path)
    print_kv("CEX 数据源", fmt_optional_text(summary.cex_source, fallback="-"))
    print_kv("市场样式", fmt_optional_text(summary.market_style, fallback="-"))
    print_kv("样本区间", fmt_period(summary.start_ts_ms, summary.end_ts_ms))
    print_kv("处理分钟点数", fmt_int(summary.rows_processed))
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


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "inspect-db":
        inspect_db(args.db_path, args.show_failures)
        return

    if args.command == "run":
        cfg = RunConfig(
            db_path=args.db_path,
            market_id=args.market_id,
            token_id=args.token_id,
            start_ts_ms=parse_time_to_ms(args.start),
            end_ts_ms=parse_time_to_ms(args.end),
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
            report_json=args.report_json,
            equity_csv=args.equity_csv,
        )
        summary, equity_curve, market_rankings = run_backtest(cfg)
        print_summary(summary, cfg, market_rankings)

        if cfg.report_json:
            payload = {
                "summary": asdict(summary),
                "market_rankings": [market_payload(row) for row in market_rankings],
                "equity_points": [asdict(point) for point in equity_curve],
            }
            dump_json(cfg.report_json, payload)
            print()
            print_kv("已写出 report_json", cfg.report_json)

        if cfg.equity_csv:
            dump_equity_csv(cfg.equity_csv, equity_curve)
            print_kv("已写出 equity_csv", cfg.equity_csv)
        return

    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
