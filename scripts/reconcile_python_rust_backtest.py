#!/usr/bin/env python3
"""
对账 Python 旧回测 与 Rust replay。

目标不是再造一套新策略，而是把“同一批数据、同一组参数”下，
旧 Python token 级持仓和市场级单仓位口径拆开看清楚。
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
LEGACY_SCRIPT_PATH = REPO_ROOT / "scripts" / "btc_basis_backtest_report.py"
DEFAULT_DB_PATH = "data/btc_basis_backtest_price_only_2025_2026.duckdb"
DEFAULT_RUST_REPORT_PATH = "data/reports/rust-replay-report.json"


def load_legacy_module() -> Any:
    spec = importlib.util.spec_from_file_location("legacy_backtest_report", LEGACY_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load legacy backtest module: {LEGACY_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


LEGACY = load_legacy_module()
RUNTIME_DEFAULTS = LEGACY.load_runtime_backtest_defaults()


@dataclass
class SignalState:
    event_id: str
    market_id: str
    history: deque[float]
    last_price: float = 0.0
    last_cex_price: float = 0.0
    has_last_price: bool = False
    last_signal_basis: float = 0.0


@dataclass
class PositionState:
    event_id: str
    market_id: str
    token_id: str
    token_side: str
    position_open: bool = False
    poly_entry_price: float = 0.0
    poly_entry_notional: float = 0.0
    shares: float = 0.0
    last_price: float = 0.0
    cex_entry_price: float = 0.0
    cex_qty_btc: float = 0.0
    reserved_margin: float = 0.0
    last_cex_price: float = 0.0
    round_trip_pnl: float = 0.0
    overlap_entry: bool = False


@dataclass(frozen=True)
class Snapshot:
    observation: Any
    fair_value: float
    signal_basis: float
    zscore: float | None
    delta: float


@dataclass
class PortfolioResult:
    name: str
    ending_equity: float
    total_pnl: float
    trade_count: int
    poly_trade_count: int
    cex_trade_count: int
    entry_signal_attempts: int
    entry_capital_reject_count: int
    exit_signal_count: int
    round_trip_count: int
    winning_round_trips: int
    win_rate: float
    max_drawdown: float
    max_gross_exposure: float
    max_reserved_margin: float
    max_capital_in_use: float
    max_capital_utilization: float
    overlap_entry_count: int
    overlap_round_trip_count: int
    overlap_round_trip_pnl: float
    overlap_signal_attempts: int
    overlap_blocked_count: int
    same_minute_reentry_attempts: int
    same_minute_reentry_blocked_count: int
    simultaneous_market_minutes: int
    simultaneous_market_instances: int
    simultaneous_market_count: int
    max_open_tokens_same_market: int
    final_open_positions: int
    top_overlap_markets: list[dict[str, Any]]


class PortfolioLedger:
    def __init__(
        self,
        name: str,
        cfg: Any,
        contract_specs: dict[str, Any],
        *,
        block_market_overlap: bool,
        block_same_minute_reentry: bool,
    ) -> None:
        self.name = name
        self.cfg = cfg
        self.contract_specs = contract_specs
        self.block_market_overlap = block_market_overlap
        self.block_same_minute_reentry = block_same_minute_reentry

        self.positions: dict[str, PositionState] = {}
        self.cash = cfg.initial_capital

        self.trade_count = 0
        self.poly_trade_count = 0
        self.cex_trade_count = 0
        self.entry_signal_attempts = 0
        self.entry_capital_reject_count = 0
        self.exit_signal_count = 0
        self.round_trip_count = 0
        self.winning_round_trips = 0
        self.max_gross_exposure = 0.0
        self.max_reserved_margin = 0.0
        self.max_capital_in_use = 0.0
        self.max_capital_utilization = 0.0
        self.peak_equity = cfg.initial_capital
        self.max_drawdown = 0.0

        self.overlap_signal_attempts = 0
        self.overlap_entry_count = 0
        self.overlap_blocked_count = 0
        self.same_minute_reentry_attempts = 0
        self.same_minute_reentry_blocked_count = 0
        self.overlap_round_trip_count = 0
        self.overlap_round_trip_pnl = 0.0
        self.overlap_market_counter: Counter[str] = Counter()
        self.simultaneous_market_minutes = 0
        self.simultaneous_market_instances = 0
        self.simultaneous_market_ids: set[str] = set()
        self.max_open_tokens_same_market = 0

    def ensure_position(self, token_id: str) -> PositionState:
        existing = self.positions.get(token_id)
        if existing is not None:
            return existing

        contract = self.contract_specs[token_id]
        state = PositionState(
            event_id=contract.event_id,
            market_id=contract.market_id,
            token_id=token_id,
            token_side=contract.token_side,
        )
        self.positions[token_id] = state
        return state

    def mark_cex_price(self, current_cex_price: float) -> None:
        for position in self.positions.values():
            if position.position_open:
                position.last_cex_price = current_cex_price

    def mark_poly_price(self, token_id: str, poly_price: float, current_cex_price: float) -> None:
        position = self.positions.get(token_id)
        if position is None:
            return
        position.last_price = poly_price
        position.last_cex_price = current_cex_price

    def portfolio_usage(self) -> tuple[float, float]:
        reserved = 0.0
        capital = 0.0
        for position in self.positions.values():
            if not position.position_open:
                continue
            reserved += position.reserved_margin
            capital += position.poly_entry_notional + position.reserved_margin
        return reserved, capital

    def market_open_tokens(self, market_id: str, *, exclude_token_id: str | None = None) -> list[PositionState]:
        out: list[PositionState] = []
        for token_id, position in self.positions.items():
            if exclude_token_id is not None and token_id == exclude_token_id:
                continue
            if position.position_open and position.market_id == market_id:
                out.append(position)
        return out

    def close_position(
        self,
        token_id: str,
        current_cex_price: float,
        *,
        market_exit_price: float | None = None,
        settle_to_payout: bool = False,
    ) -> None:
        state = self.positions.get(token_id)
        if state is None or not state.position_open:
            return

        contract = self.contract_specs[token_id]
        exit_cex_price = float(current_cex_price)

        if settle_to_payout:
            payout = contract.resolved_payout
            if payout is None:
                payout = LEGACY.token_fair_value(contract, exit_cex_price, None, 0.0)
            sale_value = state.shares * payout
            poly_gross_pnl = sale_value - state.poly_entry_notional
        else:
            sale_price = float(market_exit_price if market_exit_price is not None else state.last_price)
            sale_value = state.shares * sale_price
            poly_gross_pnl = state.shares * (sale_price - state.poly_entry_price)

        self.cash += sale_value
        state.round_trip_pnl += poly_gross_pnl

        if not settle_to_payout:
            self.cash, _, _ = LEGACY.apply_trade_costs(
                self.cash,
                sale_value,
                self.cfg.fee_bps,
                self.cfg.slippage_bps,
            )
            exit_cost = abs(sale_value) * (self.cfg.fee_bps + self.cfg.slippage_bps) / 10_000.0
            state.round_trip_pnl -= exit_cost
            self.trade_count += 1
            self.poly_trade_count += 1

        cex_gross_pnl = LEGACY.linear_cex_pnl(state.cex_entry_price, exit_cex_price, state.cex_qty_btc)
        self.cash += cex_gross_pnl
        state.round_trip_pnl += cex_gross_pnl
        exit_cex_notional = abs(state.cex_qty_btc) * exit_cex_price
        if exit_cex_notional > 0.0:
            self.cash, _, _ = LEGACY.apply_trade_costs(
                self.cash,
                exit_cex_notional,
                self.cfg.fee_bps,
                self.cfg.slippage_bps,
            )
            cex_exit_cost = exit_cex_notional * (self.cfg.fee_bps + self.cfg.slippage_bps) / 10_000.0
            state.round_trip_pnl -= cex_exit_cost
            self.trade_count += 1
            self.cex_trade_count += 1

        self.round_trip_count += 1
        if state.round_trip_pnl > 0.0:
            self.winning_round_trips += 1
        if state.overlap_entry:
            self.overlap_round_trip_count += 1
            self.overlap_round_trip_pnl += state.round_trip_pnl

        state.position_open = False
        state.poly_entry_price = 0.0
        state.poly_entry_notional = 0.0
        state.shares = 0.0
        state.cex_entry_price = 0.0
        state.cex_qty_btc = 0.0
        state.reserved_margin = 0.0
        state.round_trip_pnl = 0.0
        state.overlap_entry = False

    def process_minute(self, snapshots: list[Snapshot], current_cex_price: float, ts_ms: int) -> None:
        exit_candidates: list[Snapshot] = []
        entry_candidates: list[Snapshot] = []
        exited_market_ids: set[str] = set()

        for snapshot in snapshots:
            observation = snapshot.observation
            token_id = observation.token_id
            position = self.ensure_position(token_id)

            should_exit = (
                position.position_open
                and snapshot.zscore is not None
                and (abs(snapshot.zscore) <= self.cfg.exit_z or snapshot.zscore >= self.cfg.entry_z)
            )
            should_enter = (
                not position.position_open
                and max((observation.contract.end_ts_ms - observation.ts_ms) / 60_000.0, 0.0)
                > LEGACY.MIN_MINUTES_TO_EXPIRY
                and snapshot.zscore is not None
                and snapshot.zscore <= -self.cfg.entry_z
                and observation.poly_price > 0.0
                and observation.cex_price > 0.0
            )

            if should_exit:
                exit_candidates.append(snapshot)
            elif should_enter:
                entry_candidates.append(snapshot)

        for snapshot in exit_candidates:
            self.exit_signal_count += 1
            exited_market_ids.add(snapshot.observation.market_id)
            self.close_position(
                snapshot.observation.token_id,
                current_cex_price,
                market_exit_price=snapshot.observation.poly_price,
                settle_to_payout=False,
            )

        expired_token_ids = [
            token_id
            for token_id, position in self.positions.items()
            if position.position_open and self.contract_specs[token_id].end_ts_ms <= ts_ms
        ]
        for token_id in expired_token_ids:
            exited_market_ids.add(self.contract_specs[token_id].market_id)
            self.close_position(token_id, current_cex_price, settle_to_payout=True)

        capital_limit = self.cfg.initial_capital * max(self.cfg.max_capital_usage, 0.0)
        ordered_entries = sorted(
            entry_candidates,
            key=lambda item: (
                item.zscore if item.zscore is not None else 0.0,
                -abs(item.signal_basis),
            ),
        )

        for snapshot in ordered_entries:
            self.entry_signal_attempts += 1
            observation = snapshot.observation
            token_id = observation.token_id
            position = self.ensure_position(token_id)
            if position.position_open:
                continue

            sibling_positions = self.market_open_tokens(observation.market_id, exclude_token_id=token_id)
            is_overlap_entry = bool(sibling_positions)
            if is_overlap_entry:
                self.overlap_signal_attempts += 1
            if self.block_market_overlap and is_overlap_entry:
                self.overlap_blocked_count += 1
                continue
            if observation.market_id in exited_market_ids:
                self.same_minute_reentry_attempts += 1
                if self.block_same_minute_reentry:
                    self.same_minute_reentry_blocked_count += 1
                    continue

            shares, poly_entry_notional, _ = LEGACY.resolve_position_size(self.cfg, observation.poly_price)
            hedge_qty_btc = -shares * snapshot.delta * max(self.cfg.cex_hedge_ratio, 0.0)
            hedge_entry_notional = abs(hedge_qty_btc) * observation.cex_price
            margin_required = hedge_entry_notional * max(self.cfg.cex_margin_ratio, 0.0)
            reserved_margin_total, capital_in_use = self.portfolio_usage()
            available_cash = self.cash - reserved_margin_total
            estimated_entry_fees = (
                poly_entry_notional * (self.cfg.fee_bps + self.cfg.slippage_bps) / 10_000.0
                + hedge_entry_notional * (self.cfg.fee_bps + self.cfg.slippage_bps) / 10_000.0
            )
            required_cash = poly_entry_notional + margin_required + estimated_entry_fees

            if (
                shares <= 0.0
                or poly_entry_notional <= 0.0
                or available_cash < required_cash
                or capital_in_use + poly_entry_notional + margin_required > capital_limit + 1e-12
            ):
                self.entry_capital_reject_count += 1
                continue

            self.cash -= poly_entry_notional
            self.cash, _, _ = LEGACY.apply_trade_costs(
                self.cash,
                poly_entry_notional,
                self.cfg.fee_bps,
                self.cfg.slippage_bps,
            )
            position.round_trip_pnl -= poly_entry_notional * (self.cfg.fee_bps + self.cfg.slippage_bps) / 10_000.0
            self.trade_count += 1
            self.poly_trade_count += 1

            if hedge_entry_notional > 0.0:
                self.cash, _, _ = LEGACY.apply_trade_costs(
                    self.cash,
                    hedge_entry_notional,
                    self.cfg.fee_bps,
                    self.cfg.slippage_bps,
                )
                position.round_trip_pnl -= hedge_entry_notional * (
                    self.cfg.fee_bps + self.cfg.slippage_bps
                ) / 10_000.0
                self.trade_count += 1
                self.cex_trade_count += 1

            position.position_open = True
            position.poly_entry_price = observation.poly_price
            position.poly_entry_notional = poly_entry_notional
            position.shares = shares
            position.last_price = observation.poly_price
            position.cex_entry_price = observation.cex_price
            position.cex_qty_btc = hedge_qty_btc
            position.last_cex_price = current_cex_price
            position.reserved_margin = margin_required
            position.overlap_entry = is_overlap_entry

            if is_overlap_entry:
                self.overlap_entry_count += 1
                self.overlap_market_counter[observation.market_id] += 1

    def record_equity(self) -> float:
        reserved_margin_total, capital_in_use = self.portfolio_usage()
        gross_exposure = 0.0
        market_value = 0.0
        cex_unrealized_total = 0.0
        open_by_market: dict[str, list[PositionState]] = defaultdict(list)

        for position in self.positions.values():
            if not position.position_open:
                continue
            current_poly_value = position.shares * position.last_price
            current_cex_unrealized = LEGACY.linear_cex_pnl(
                position.cex_entry_price,
                position.last_cex_price,
                position.cex_qty_btc,
            )
            market_value += current_poly_value
            cex_unrealized_total += current_cex_unrealized
            gross_exposure += current_poly_value + abs(position.cex_qty_btc) * position.last_cex_price
            open_by_market[position.market_id].append(position)

        simultaneous_instances = 0
        for market_id, positions in open_by_market.items():
            self.max_open_tokens_same_market = max(self.max_open_tokens_same_market, len(positions))
            open_sides = {position.token_side.lower() for position in positions}
            if len(open_sides) > 1:
                simultaneous_instances += 1
                self.simultaneous_market_ids.add(market_id)

        if simultaneous_instances > 0:
            self.simultaneous_market_minutes += 1
            self.simultaneous_market_instances += simultaneous_instances

        self.max_gross_exposure = max(self.max_gross_exposure, gross_exposure)
        self.max_reserved_margin = max(self.max_reserved_margin, reserved_margin_total)
        self.max_capital_in_use = max(self.max_capital_in_use, capital_in_use)
        if abs(self.cfg.initial_capital) > 1e-12:
            self.max_capital_utilization = max(
                self.max_capital_utilization,
                capital_in_use / self.cfg.initial_capital,
            )

        equity = self.cash + market_value + cex_unrealized_total
        self.peak_equity = max(self.peak_equity, equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - equity)
        return equity

    def finalize(self, last_ts_ms: int, last_cex_price: float) -> PortfolioResult:
        remaining_token_ids = [
            token_id for token_id, position in self.positions.items() if position.position_open
        ]
        for token_id in remaining_token_ids:
            self.close_position(token_id, last_cex_price, settle_to_payout=True)

        ending_equity = self.cash
        win_rate = 0.0 if self.round_trip_count <= 0 else self.winning_round_trips / self.round_trip_count
        top_overlap_markets = [
            {"market_id": market_id, "overlap_entries": count}
            for market_id, count in self.overlap_market_counter.most_common(10)
        ]
        return PortfolioResult(
            name=self.name,
            ending_equity=ending_equity,
            total_pnl=ending_equity - self.cfg.initial_capital,
            trade_count=self.trade_count,
            poly_trade_count=self.poly_trade_count,
            cex_trade_count=self.cex_trade_count,
            entry_signal_attempts=self.entry_signal_attempts,
            entry_capital_reject_count=self.entry_capital_reject_count,
            exit_signal_count=self.exit_signal_count,
            round_trip_count=self.round_trip_count,
            winning_round_trips=self.winning_round_trips,
            win_rate=win_rate,
            max_drawdown=self.max_drawdown,
            max_gross_exposure=self.max_gross_exposure,
            max_reserved_margin=self.max_reserved_margin,
            max_capital_in_use=self.max_capital_in_use,
            max_capital_utilization=self.max_capital_utilization,
            overlap_entry_count=self.overlap_entry_count,
            overlap_round_trip_count=self.overlap_round_trip_count,
            overlap_round_trip_pnl=self.overlap_round_trip_pnl,
            overlap_signal_attempts=self.overlap_signal_attempts,
            overlap_blocked_count=self.overlap_blocked_count,
            same_minute_reentry_attempts=self.same_minute_reentry_attempts,
            same_minute_reentry_blocked_count=self.same_minute_reentry_blocked_count,
            simultaneous_market_minutes=self.simultaneous_market_minutes,
            simultaneous_market_instances=self.simultaneous_market_instances,
            simultaneous_market_count=len(self.simultaneous_market_ids),
            max_open_tokens_same_market=self.max_open_tokens_same_market,
            final_open_positions=0,
            top_overlap_markets=top_overlap_markets,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="对账 Python 旧回测 与 Rust replay")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="DuckDB 路径")
    parser.add_argument("--market-id", default=None, help="只看单个 market_id")
    parser.add_argument("--token-id", default=None, help="只看单个 token_id")
    parser.add_argument("--start", default=None, help="开始时间，支持 YYYY-MM-DD 或毫秒时间戳")
    parser.add_argument("--end", default=None, help="结束时间，支持 YYYY-MM-DD 或毫秒时间戳")
    parser.add_argument("--initial-capital", type=float, default=RUNTIME_DEFAULTS.initial_capital)
    parser.add_argument("--rolling-window", type=int, default=RUNTIME_DEFAULTS.rolling_window)
    parser.add_argument("--entry-z", type=float, default=RUNTIME_DEFAULTS.entry_z)
    parser.add_argument("--exit-z", type=float, default=RUNTIME_DEFAULTS.exit_z)
    parser.add_argument("--position-units", type=float, default=None)
    parser.add_argument(
        "--position-notional-usd",
        type=float,
        default=RUNTIME_DEFAULTS.position_notional_usd,
    )
    parser.add_argument("--max-capital-usage", type=float, default=RUNTIME_DEFAULTS.max_capital_usage)
    parser.add_argument("--cex-hedge-ratio", type=float, default=1.0)
    parser.add_argument("--cex-margin-ratio", type=float, default=0.10)
    parser.add_argument("--fee-bps", type=float, default=2.0)
    parser.add_argument("--slippage-bps", type=float, default=RUNTIME_DEFAULTS.poly_slippage_bps)
    parser.add_argument(
        "--rust-report-json",
        default=DEFAULT_RUST_REPORT_PATH,
        help="Rust replay 报告 JSON；不存在时只输出 Python 侧对账",
    )
    parser.add_argument("--report-json", default=None, help="把对账结果写入 JSON")
    return parser


def build_run_config(args: argparse.Namespace) -> Any:
    return LEGACY.RunConfig(
        db_path=args.db_path,
        market_id=args.market_id,
        token_id=args.token_id,
        start_ts_ms=LEGACY.parse_time_to_ms(args.start),
        end_ts_ms=LEGACY.parse_time_to_ms(args.end),
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
        poly_slippage_bps=float(args.slippage_bps),
        cex_slippage_bps=RUNTIME_DEFAULTS.cex_slippage_bps,
        min_poly_price=RUNTIME_DEFAULTS.min_poly_price,
        max_poly_price=RUNTIME_DEFAULTS.max_poly_price,
        report_json=None,
        equity_csv=None,
    )


def run_reconciliation(cfg: Any) -> dict[str, Any]:
    inputs = LEGACY.prepare_backtest_inputs(cfg)
    contract_specs = inputs.contract_specs
    observations_by_minute = inputs.observations_by_minute
    minute_keys = sorted(observations_by_minute)
    if not minute_keys:
        raise SystemExit("no aligned minutes found")

    signal_states: dict[str, SignalState] = {}
    ledgers = [
        PortfolioLedger(
            "python_legacy_token_level",
            cfg,
            contract_specs,
            block_market_overlap=False,
            block_same_minute_reentry=False,
        ),
        PortfolioLedger(
            "python_market_level_shadow",
            cfg,
            contract_specs,
            block_market_overlap=True,
            block_same_minute_reentry=False,
        ),
        PortfolioLedger(
            "python_market_level_no_same_minute_reentry",
            cfg,
            contract_specs,
            block_market_overlap=True,
            block_same_minute_reentry=True,
        ),
    ]

    for ts_ms in minute_keys:
        batch = observations_by_minute[ts_ms]
        current_cex_price = batch[0].cex_price
        for ledger in ledgers:
            ledger.mark_cex_price(current_cex_price)

        snapshots: list[Snapshot] = []
        for observation in batch:
            token_id = observation.token_id
            signal_state = signal_states.get(token_id)
            if signal_state is None:
                signal_state = SignalState(
                    event_id=observation.contract.event_id,
                    market_id=observation.contract.market_id,
                    history=deque(maxlen=cfg.rolling_window),
                )
                signal_states[token_id] = signal_state

            signal_state.last_price = observation.poly_price
            signal_state.last_cex_price = current_cex_price
            signal_state.has_last_price = True

            for ledger in ledgers:
                ledger.mark_poly_price(token_id, observation.poly_price, current_cex_price)

            minutes_to_expiry = max((observation.contract.end_ts_ms - observation.ts_ms) / 60_000.0, 0.0)
            fair_value = LEGACY.token_fair_value(
                observation.contract,
                observation.cex_price,
                observation.sigma,
                minutes_to_expiry,
            )
            delta = LEGACY.token_delta(
                observation.contract,
                observation.cex_price,
                observation.sigma,
                minutes_to_expiry,
            )
            signal_basis = observation.poly_price - fair_value
            zscore = LEGACY.rolling_zscore(signal_state.history, signal_basis)
            snapshots.append(
                Snapshot(
                    observation=observation,
                    fair_value=fair_value,
                    signal_basis=signal_basis,
                    zscore=zscore,
                    delta=delta,
                )
            )
            signal_state.last_signal_basis = signal_basis

        for ledger in ledgers:
            ledger.process_minute(snapshots, current_cex_price, ts_ms)

        for snapshot in snapshots:
            signal_states[snapshot.observation.token_id].history.append(snapshot.signal_basis)

        for ledger in ledgers:
            ledger.record_equity()

    last_ts_ms = minute_keys[-1]
    last_cex_price = observations_by_minute[last_ts_ms][0].cex_price
    return {
        "db_path": cfg.db_path,
        "minute_count": len(minute_keys),
        "market_count": len({ob.contract.market_id for batch in observations_by_minute.values() for ob in batch}),
        "event_count": len({ob.contract.event_id for batch in observations_by_minute.values() for ob in batch}),
        "rows_processed": sum(len(batch) for batch in observations_by_minute.values()),
        "portfolios": [asdict(ledger.finalize(last_ts_ms, last_cex_price)) for ledger in ledgers],
    }


def load_rust_report(path: str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    report_path = Path(path)
    if not report_path.is_absolute():
        report_path = REPO_ROOT / report_path
    if not report_path.exists():
        return None
    payload = json.loads(report_path.read_text())
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else None


def find_portfolio(result: dict[str, Any], name: str) -> dict[str, Any]:
    for item in result["portfolios"]:
        if item["name"] == name:
            return item
    raise KeyError(name)


def print_money(label: str, value: float) -> None:
    print(f"  {label}: {value:,.4f}")


def print_int(label: str, value: int) -> None:
    print(f"  {label}: {value:,}")


def print_portfolio(title: str, item: dict[str, Any]) -> None:
    print(title)
    print_money("ending_equity", item["ending_equity"])
    print_money("total_pnl", item["total_pnl"])
    print_int("trade_count", item["trade_count"])
    print_int("poly_trade_count", item["poly_trade_count"])
    print_int("cex_trade_count", item["cex_trade_count"])
    print_int("entry_signal_attempts", item["entry_signal_attempts"])
    print_int("entry_capital_reject_count", item["entry_capital_reject_count"])
    print_int("exit_signal_count", item["exit_signal_count"])
    print_int("round_trip_count", item["round_trip_count"])
    print(f"  win_rate: {item['win_rate']:.2%}")
    print_money("max_drawdown", item["max_drawdown"])
    print_int("overlap_entry_count", item["overlap_entry_count"])
    print_int("overlap_round_trip_count", item["overlap_round_trip_count"])
    print_money("overlap_round_trip_pnl", item["overlap_round_trip_pnl"])
    print_int("overlap_signal_attempts", item["overlap_signal_attempts"])
    print_int("overlap_blocked_count", item["overlap_blocked_count"])
    print_int("same_minute_reentry_attempts", item["same_minute_reentry_attempts"])
    print_int("same_minute_reentry_blocked_count", item["same_minute_reentry_blocked_count"])
    print_int("simultaneous_market_minutes", item["simultaneous_market_minutes"])
    print_int("simultaneous_market_instances", item["simultaneous_market_instances"])
    print_int("simultaneous_market_count", item["simultaneous_market_count"])
    print_int("max_open_tokens_same_market", item["max_open_tokens_same_market"])
    if item["top_overlap_markets"]:
        print("  top_overlap_markets:")
        for row in item["top_overlap_markets"]:
            print(f"    - {row['market_id']}: {row['overlap_entries']}")


def build_conclusion(result: dict[str, Any], rust_summary: dict[str, Any] | None) -> list[str]:
    legacy = find_portfolio(result, "python_legacy_token_level")
    shadow = find_portfolio(result, "python_market_level_shadow")
    no_flip = find_portfolio(result, "python_market_level_no_same_minute_reentry")
    lines = []

    pnl_gap = legacy["total_pnl"] - shadow["total_pnl"]
    lines.append(
        f"Python 旧回测比市场级影子组合多出 {pnl_gap:,.4f} 美元 PnL，核心差异来自同市场重叠持仓。"
    )
    if legacy["simultaneous_market_minutes"] > 0:
        lines.append(
            "旧 Python 口径确实会在同一市场里同时持有 YES/NO 或者持有多个 token 仓位，这和 Rust 的单市场单仓位模型不一致。"
        )
    if legacy["overlap_round_trip_count"] > 0:
        lines.append(
            f"其中重叠开仓贡献了 {legacy['overlap_round_trip_count']:,} 个 round trip，净 PnL {legacy['overlap_round_trip_pnl']:,.4f} 美元。"
        )
    if no_flip["same_minute_reentry_blocked_count"] > 0:
        lines.append(
            f"旧 Python 口径里还有 {no_flip['same_minute_reentry_blocked_count']:,} 次“同一分钟先平再反手/再开”机会。"
        )

    if rust_summary is not None:
        rust_pnl = float(rust_summary.get("total_pnl", 0.0))
        rust_round = int(rust_summary.get("round_trip_count", 0))
        shadow_gap = shadow["total_pnl"] - rust_pnl
        shadow_round_gap = shadow["round_trip_count"] - rust_round
        no_flip_gap = no_flip["total_pnl"] - rust_pnl
        no_flip_round_gap = no_flip["round_trip_count"] - rust_round
        if abs(no_flip_gap) < abs(shadow_gap):
            lines.append(
                "禁止同分钟反手后更接近 Rust replay，说明 Rust 目前更像“每市场每分钟最多一个动作”的执行模型。"
            )
        else:
            lines.append(
                "即使禁止同分钟反手，Python 影子组合和 Rust replay 仍然差很远，说明 Rust 侧还存在其他执行路径差异。"
            )
        lines.append(
            f"影子组合相对 Rust 的差距为 PnL {shadow_gap:,.4f} 美元、round trip {shadow_round_gap:,} 笔。"
        )
        lines.append(
            f"禁止同分钟反手后的影子组合相对 Rust 差距为 PnL {no_flip_gap:,.4f} 美元、round trip {no_flip_round_gap:,} 笔。"
        )

    return lines


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    cfg = build_run_config(args)
    result = run_reconciliation(cfg)
    rust_summary = load_rust_report(args.rust_report_json)

    print("=== 对账样本 ===")
    print(f"  db_path: {result['db_path']}")
    print_int("minute_count", int(result["minute_count"]))
    print_int("market_count", int(result["market_count"]))
    print_int("event_count", int(result["event_count"]))
    print_int("rows_processed", int(result["rows_processed"]))
    print()

    print_portfolio("=== Python 旧口径 ===", find_portfolio(result, "python_legacy_token_level"))
    print()
    print_portfolio("=== Python 市场级影子组合 ===", find_portfolio(result, "python_market_level_shadow"))
    print()
    print_portfolio(
        "=== Python 市场级 + 禁止同分钟反手 ===",
        find_portfolio(result, "python_market_level_no_same_minute_reentry"),
    )

    if rust_summary is not None:
        print()
        print("=== Rust Replay 报告 ===")
        print_money("ending_equity", float(rust_summary.get("ending_equity", 0.0)))
        print_money("total_pnl", float(rust_summary.get("total_pnl", 0.0)))
        print_int("trade_count", int(rust_summary.get("trade_count", 0)))
        print_int("poly_trade_count", int(rust_summary.get("poly_trade_count", 0)))
        print_int("cex_trade_count", int(rust_summary.get("cex_trade_count", 0)))
        print_int("round_trip_count", int(rust_summary.get("round_trip_count", 0)))
        print(f"  win_rate: {float(rust_summary.get('win_rate', 0.0)):.2%}")
        print_money("max_drawdown", float(rust_summary.get("max_drawdown", 0.0)))

    conclusion = build_conclusion(result, rust_summary)
    print()
    print("=== 结论 ===")
    for line in conclusion:
        print(f"  - {line}")

    output = {
        "result": result,
        "rust_summary": rust_summary,
        "conclusion": conclusion,
    }
    if args.report_json:
        report_path = Path(args.report_json)
        if not report_path.is_absolute():
            report_path = REPO_ROOT / report_path
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(output, ensure_ascii=False, indent=2))
        print()
        print(f"已写入: {report_path}")


if __name__ == "__main__":
    main()
