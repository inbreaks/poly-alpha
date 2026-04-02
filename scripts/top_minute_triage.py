#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Callable

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - unit tests install a stub module
    duckdb = None


ONE_MINUTE_MS = 60_000
SYNC_RESET_NEUTRAL_TARGET = 0.50
SYNC_RESET_NEUTRAL_TOLERANCE = 0.055
SYNC_RESET_INTERIOR_MIN = 0.25
SYNC_RESET_INTERIOR_MAX = 0.75
SYNC_RESET_MIN_MOVE = 0.25


@dataclass(frozen=True)
class StressConfig:
    poly_fee_bps: float
    cex_fee_bps: float


@dataclass(frozen=True)
class TradeRow:
    trade_id: str
    event_id: str
    market_id: str
    market_slug: str
    direction: str
    entry_time: int
    exit_time: int
    entry_poly_price: float
    exit_poly_price: float
    entry_cex_price: float
    exit_cex_price: float
    poly_shares: float
    cex_qty: float
    delta_at_entry: float


@dataclass(frozen=True)
class PricePoint:
    yes_price: float | None
    no_price: float | None


@dataclass(frozen=True)
class MarketSpec:
    event_id: str
    market_id: str
    market_slug: str
    market_question: str | None


@dataclass(frozen=True)
class MarketMinuteBreakdown:
    event_id: str
    market_id: str
    market_slug: str
    market_question: str | None
    total_contribution_usd: float
    fill_cash_change_usd: float
    poly_mark_change_usd: float
    cex_mark_change_usd: float
    open_positions_before: int
    open_positions_after: int
    entries_at_end_ts: int
    exits_at_end_ts: int
    yes_price_start: float | None
    yes_price_end: float | None
    no_price_start: float | None
    no_price_end: float | None
    cex_price_start: float | None
    cex_price_end: float | None


@dataclass(frozen=True)
class ClassificationResult:
    code: str
    reasons: list[str]


@dataclass(frozen=True)
class TopMinuteWindow:
    start_ts_ms: int
    end_ts_ms: int
    start_utc: str
    end_utc: str
    run_kind: str
    contributions: dict[str, float]
    aggregate_delta_usd: float


@dataclass(frozen=True)
class EquitySnapshot:
    ts_ms: int
    equity: float
    cash: float
    unrealized_pnl: float
    open_positions: int


@dataclass(frozen=True)
class RunArtifact:
    market_key: str
    db_path: str
    stress: StressConfig
    equity_by_ts: dict[int, EquitySnapshot]
    trades: list[TradeRow]


@dataclass(frozen=True)
class WindowContext:
    market_specs: dict[str, MarketSpec]
    price_series: dict[str, dict[int, PricePoint]]
    cex_close_by_ts: dict[int, float]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-triage top backtest minute moves")
    parser.add_argument("--top-minutes-csv", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-kind", default="current_noband")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--padding-minutes", type=int, default=2)
    parser.add_argument("--top-market-limit", type=int, default=3)
    return parser.parse_args(argv)


def load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_top_minutes_csv(
    path: str,
    *,
    run_kind: str,
    limit: int | None = None,
) -> list[TopMinuteWindow]:
    rows: list[TopMinuteWindow] = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            contributions = {
                field.removeprefix("new_").removesuffix("_delta"): float(value or 0.0)
                for field, value in row.items()
                if field.startswith("new_") and field.endswith("_delta")
                and field != "new_aggregate_delta"
            }
            rows.append(
                TopMinuteWindow(
                    start_ts_ms=int(row["start_ts_ms"]),
                    end_ts_ms=int(row["end_ts_ms"]),
                    start_utc=row["start_utc"],
                    end_utc=row["end_utc"],
                    run_kind=run_kind,
                    contributions=contributions,
                    aggregate_delta_usd=float(row["new_aggregate_delta"]),
                )
            )
    if limit is not None:
        return rows[:limit]
    return rows


def load_equity_snapshots(path: str) -> dict[int, EquitySnapshot]:
    rows: dict[int, EquitySnapshot] = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            snapshot = EquitySnapshot(
                ts_ms=int(float(row["ts_ms"])),
                equity=float(row["equity"]),
                cash=float(row["cash"]),
                unrealized_pnl=float(row["unrealized_pnl"]),
                open_positions=int(float(row["open_positions"])),
            )
            rows[snapshot.ts_ms] = snapshot
    return rows


def load_trade_rows(path: str) -> list[TradeRow]:
    rows: list[TradeRow] = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                TradeRow(
                    trade_id=row["trade_id"],
                    event_id=row["event_id"],
                    market_id=row["market_id"],
                    market_slug=row["market_slug"],
                    direction=row["direction"],
                    entry_time=int(float(row["entry_time"])),
                    exit_time=int(float(row["exit_time"])),
                    entry_poly_price=float(row["entry_poly_price"]),
                    exit_poly_price=float(row["exit_poly_price"]),
                    entry_cex_price=float(row["entry_cex_price"]),
                    exit_cex_price=float(row["exit_cex_price"]),
                    poly_shares=float(row["poly_shares"]),
                    cex_qty=float(row["cex_qty"]),
                    delta_at_entry=float(row["delta_at_entry"]),
                )
            )
    return rows


def discover_run_artifacts(run_dir: str) -> dict[str, RunArtifact]:
    artifacts: dict[str, RunArtifact] = {}
    if not os.path.isdir(run_dir):
        return artifacts

    for name in sorted(os.listdir(run_dir)):
        if not name.endswith("-report.json"):
            continue
        market_key = name.removesuffix("-report.json")
        report_path = os.path.join(run_dir, name)
        equity_path = os.path.join(run_dir, f"{market_key}-equity.csv")
        trades_path = os.path.join(run_dir, f"{market_key}-trades.csv")
        report = load_json(report_path)
        summary = report.get("summary", {})
        stress = summary.get("stress", {})
        artifacts[market_key] = RunArtifact(
            market_key=market_key,
            db_path=str(summary["db_path"]),
            stress=StressConfig(
                poly_fee_bps=float(stress.get("poly_fee_bps", 0.0)),
                cex_fee_bps=float(stress.get("cex_fee_bps", 0.0)),
            ),
            equity_by_ts=load_equity_snapshots(equity_path),
            trades=load_trade_rows(trades_path) if os.path.exists(trades_path) else [],
        )
    return artifacts


def token_side_from_trade(trade: TradeRow) -> str:
    return "yes" if trade.direction.startswith("yes_") else "no"


def signed_cex_qty_from_trade(trade: TradeRow) -> float:
    return -trade.cex_qty if trade.delta_at_entry >= 0.0 else trade.cex_qty


def linear_cex_pnl(entry_price: float, exit_price: float, qty: float) -> float:
    if entry_price <= 0.0 or abs(qty) <= 1e-12:
        return 0.0
    return qty * (exit_price - entry_price)


def replay_entry_fee_usd(trade: TradeRow, stress: StressConfig) -> float:
    return (
        trade.poly_shares * trade.entry_poly_price * stress.poly_fee_bps / 10_000.0
        + trade.cex_qty * trade.entry_cex_price * stress.cex_fee_bps / 10_000.0
    )


def replay_exit_fee_usd(trade: TradeRow, stress: StressConfig) -> float:
    return (
        trade.poly_shares * trade.exit_poly_price * stress.poly_fee_bps / 10_000.0
        + trade.cex_qty * trade.exit_cex_price * stress.cex_fee_bps / 10_000.0
    )


def replay_reference_cex_price_at(
    cex_close_by_ts: dict[int, float], ts_ms: int
) -> float | None:
    prior_ts_ms = ts_ms - ONE_MINUTE_MS
    if prior_ts_ms < 0:
        return None
    return cex_close_by_ts.get(prior_ts_ms)


def price_point_at(
    price_series: dict[str, dict[int, PricePoint]],
    market_id: str,
    ts_ms: int,
) -> PricePoint | None:
    return price_series.get(market_id, {}).get(ts_ms)


def replay_trade_component_at(
    price_series: dict[str, dict[int, PricePoint]],
    cex_close_by_ts: dict[int, float],
    trade: TradeRow,
    ts_ms: int,
) -> tuple[float, float] | None:
    ts = ts_ms // 1000
    if not (trade.entry_time <= ts and trade.exit_time > ts):
        return None
    if trade.entry_time == ts:
        return (trade.poly_shares * trade.entry_poly_price, 0.0)

    prices = price_point_at(price_series, trade.market_id, ts_ms)
    if prices is None:
        return None
    poly_price = prices.yes_price if token_side_from_trade(trade) == "yes" else prices.no_price
    if poly_price is None:
        return None
    cex_price = replay_reference_cex_price_at(cex_close_by_ts, ts_ms)
    if cex_price is None:
        return None
    return (
        trade.poly_shares * poly_price,
        linear_cex_pnl(
            trade.entry_cex_price,
            cex_price,
            signed_cex_qty_from_trade(trade),
        ),
    )


def build_market_minute_breakdown(
    *,
    trades: list[TradeRow],
    market_specs: dict[str, MarketSpec],
    price_series: dict[str, dict[int, PricePoint]],
    cex_close_by_ts: dict[int, float],
    stress: StressConfig,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[MarketMinuteBreakdown]:
    start_ts = start_ts_ms // 1000
    end_ts = end_ts_ms // 1000
    accumulators: dict[str, dict[str, float | int]] = {}

    for trade in trades:
        if not (trade.entry_time <= end_ts and trade.exit_time > start_ts):
            continue
        acc = accumulators.setdefault(
            trade.market_id,
            {
                "fill_cash_change_usd": 0.0,
                "poly_mark_change_usd": 0.0,
                "cex_mark_change_usd": 0.0,
                "open_positions_before": 0,
                "open_positions_after": 0,
                "entries_at_end_ts": 0,
                "exits_at_end_ts": 0,
            },
        )

        if trade.entry_time <= start_ts and trade.exit_time > start_ts:
            acc["open_positions_before"] = int(acc["open_positions_before"]) + 1
        if trade.entry_time <= end_ts and trade.exit_time > end_ts:
            acc["open_positions_after"] = int(acc["open_positions_after"]) + 1

        before = replay_trade_component_at(price_series, cex_close_by_ts, trade, start_ts_ms)
        after = replay_trade_component_at(price_series, cex_close_by_ts, trade, end_ts_ms)
        before_poly, before_cex = before or (0.0, 0.0)
        after_poly, after_cex = after or (0.0, 0.0)
        acc["poly_mark_change_usd"] = float(acc["poly_mark_change_usd"]) + (
            after_poly - before_poly
        )
        acc["cex_mark_change_usd"] = float(acc["cex_mark_change_usd"]) + (
            after_cex - before_cex
        )

        if trade.entry_time == end_ts:
            acc["entries_at_end_ts"] = int(acc["entries_at_end_ts"]) + 1
            acc["fill_cash_change_usd"] = float(acc["fill_cash_change_usd"]) - (
                trade.poly_shares * trade.entry_poly_price
                + replay_entry_fee_usd(trade, stress)
            )
        if trade.exit_time == end_ts:
            acc["exits_at_end_ts"] = int(acc["exits_at_end_ts"]) + 1
            acc["fill_cash_change_usd"] = float(acc["fill_cash_change_usd"]) + (
                trade.poly_shares * trade.exit_poly_price
                + linear_cex_pnl(
                    trade.entry_cex_price,
                    trade.exit_cex_price,
                    signed_cex_qty_from_trade(trade),
                )
                - replay_exit_fee_usd(trade, stress)
            )

    rows: list[MarketMinuteBreakdown] = []
    for market_id, acc in accumulators.items():
        spec = market_specs.get(
            market_id,
            MarketSpec(
                event_id="",
                market_id=market_id,
                market_slug=market_id,
                market_question=None,
            ),
        )
        start_prices = price_point_at(price_series, market_id, start_ts_ms)
        end_prices = price_point_at(price_series, market_id, end_ts_ms)
        fill_cash_change_usd = float(acc["fill_cash_change_usd"])
        poly_mark_change_usd = float(acc["poly_mark_change_usd"])
        cex_mark_change_usd = float(acc["cex_mark_change_usd"])
        rows.append(
            MarketMinuteBreakdown(
                event_id=spec.event_id,
                market_id=market_id,
                market_slug=spec.market_slug,
                market_question=spec.market_question,
                total_contribution_usd=(
                    fill_cash_change_usd + poly_mark_change_usd + cex_mark_change_usd
                ),
                fill_cash_change_usd=fill_cash_change_usd,
                poly_mark_change_usd=poly_mark_change_usd,
                cex_mark_change_usd=cex_mark_change_usd,
                open_positions_before=int(acc["open_positions_before"]),
                open_positions_after=int(acc["open_positions_after"]),
                entries_at_end_ts=int(acc["entries_at_end_ts"]),
                exits_at_end_ts=int(acc["exits_at_end_ts"]),
                yes_price_start=None if start_prices is None else start_prices.yes_price,
                yes_price_end=None if end_prices is None else end_prices.yes_price,
                no_price_start=None if start_prices is None else start_prices.no_price,
                no_price_end=None if end_prices is None else end_prices.no_price,
                cex_price_start=replay_reference_cex_price_at(cex_close_by_ts, start_ts_ms),
                cex_price_end=replay_reference_cex_price_at(cex_close_by_ts, end_ts_ms),
            )
        )
    rows.sort(
        key=lambda row: (-abs(row.total_contribution_usd), row.market_id),
    )
    return rows


def classify_minute_window(
    *,
    equity_change_usd: float,
    fill_cash_change_usd: float,
    mark_change_usd: float,
    entries_at_end_ts: int,
    exits_at_end_ts: int,
    sync_reset_market_count: int,
) -> ClassificationResult:
    abs_equity = abs(equity_change_usd)
    if abs_equity <= 1e-12:
        return ClassificationResult(code="mixed", reasons=["flat_equity"])

    mark_share = abs(mark_change_usd) / abs_equity
    fill_share = abs(fill_cash_change_usd) / abs_equity

    if sync_reset_market_count >= 3 and mark_share >= 0.70:
        return ClassificationResult(
            code="sync_reset_like",
            reasons=["mark_dominated", "multi_market_sync_reset_like"],
        )
    if exits_at_end_ts > 0 and fill_cash_change_usd > 0.0 and fill_share >= 0.60:
        return ClassificationResult(
            code="realized_close_cluster",
            reasons=["fill_dominated", "exit_cluster"],
        )
    if entries_at_end_ts > 0 and fill_cash_change_usd < 0.0 and fill_share >= 0.60:
        return ClassificationResult(
            code="entry_cluster",
            reasons=["fill_dominated", "entry_cluster"],
        )
    if entries_at_end_ts == 0 and exits_at_end_ts == 0 and mark_share >= 0.80:
        return ClassificationResult(
            code="pure_mark_shock",
            reasons=["mark_dominated", "no_fill_cluster"],
        )
    return ClassificationResult(
        code="mixed",
        reasons=["mixed_fill_and_mark"],
    )


def is_sync_reset_like_contributor(
    quote_path: dict[str, list[dict[str, float | None]]],
    *,
    end_ts_ms: int,
) -> bool:
    poly_points = quote_path["poly"]
    poly_by_ts = {int(point["ts_ms"]): point for point in poly_points}
    before = poly_by_ts.get(end_ts_ms - ONE_MINUTE_MS)
    at_end = poly_by_ts.get(end_ts_ms)
    after = poly_by_ts.get(end_ts_ms + ONE_MINUTE_MS)
    if before is None or at_end is None or after is None:
        return False

    def is_neutral(point: dict[str, float | None]) -> bool:
        yes_price = point.get("yes_price")
        no_price = point.get("no_price")
        if yes_price is None or no_price is None:
            return False
        return (
            abs(float(yes_price) - SYNC_RESET_NEUTRAL_TARGET) <= SYNC_RESET_NEUTRAL_TOLERANCE
            and abs(float(no_price) - SYNC_RESET_NEUTRAL_TARGET)
            <= SYNC_RESET_NEUTRAL_TOLERANCE
        )

    def is_interior(point: dict[str, float | None]) -> bool:
        yes_price = point.get("yes_price")
        no_price = point.get("no_price")
        if yes_price is None or no_price is None:
            return False
        return (
            SYNC_RESET_INTERIOR_MIN <= float(yes_price) <= SYNC_RESET_INTERIOR_MAX
            and SYNC_RESET_INTERIOR_MIN <= float(no_price) <= SYNC_RESET_INTERIOR_MAX
        )

    def moved(lhs: dict[str, float | None], rhs: dict[str, float | None]) -> bool:
        yes_lhs = lhs.get("yes_price")
        yes_rhs = rhs.get("yes_price")
        no_lhs = lhs.get("no_price")
        no_rhs = rhs.get("no_price")
        return (
            yes_lhs is not None
            and yes_rhs is not None
            and abs(float(yes_lhs) - float(yes_rhs)) >= SYNC_RESET_MIN_MOVE
        ) or (
            no_lhs is not None
            and no_rhs is not None
            and abs(float(no_lhs) - float(no_rhs)) >= SYNC_RESET_MIN_MOVE
        )

    return (is_neutral(at_end) or is_interior(at_end)) and moved(before, at_end) and moved(
        at_end, after
    )


def build_quote_path(
    *,
    market_id: str,
    context: WindowContext,
    start_ts_ms: int,
    end_ts_ms: int,
    padding_minutes: int,
) -> dict[str, list[dict[str, float | None]]]:
    lower = start_ts_ms - padding_minutes * ONE_MINUTE_MS
    upper = end_ts_ms + padding_minutes * ONE_MINUTE_MS
    poly_rows = [
        {
            "ts_ms": ts_ms,
            "yes_price": point.yes_price,
            "no_price": point.no_price,
        }
        for ts_ms, point in sorted(context.price_series.get(market_id, {}).items())
        if lower <= ts_ms <= upper
    ]
    cex_rows = [
        {
            "ts_ms": ts_ms,
            "price": price,
        }
        for ts_ms, price in sorted(context.cex_close_by_ts.items())
        if lower <= ts_ms <= upper
    ]
    return {"poly": poly_rows, "cex": cex_rows}


def impacted_market_ids(
    trades: list[TradeRow],
    *,
    start_ts_ms: int,
    end_ts_ms: int,
) -> list[str]:
    start_ts = start_ts_ms // 1000
    end_ts = end_ts_ms // 1000
    return sorted(
        {
            trade.market_id
            for trade in trades
            if trade.entry_time <= end_ts and trade.exit_time > start_ts
        }
    )


def equity_snapshot_at_or_before(
    equity_by_ts: dict[int, EquitySnapshot],
    ts_ms: int,
) -> EquitySnapshot | None:
    if ts_ms in equity_by_ts:
        return equity_by_ts[ts_ms]
    eligible = [key for key in equity_by_ts if key <= ts_ms]
    if not eligible:
        return None
    return equity_by_ts[max(eligible)]


def build_triage_payload(
    *,
    top_minutes: list[TopMinuteWindow],
    artifacts: dict[str, RunArtifact],
    context_provider: Callable[[str, list[str], int, int, int], WindowContext],
    padding_minutes: int,
    top_market_limit: int,
) -> dict[str, Any]:
    minutes_payload: list[dict[str, Any]] = []

    for window in top_minutes:
        minute_markets = []
        aggregate_fill = 0.0
        aggregate_mark = 0.0
        aggregate_cash_change_observed = 0.0
        aggregate_entries = 0
        aggregate_exits = 0
        aggregate_equity_change = 0.0
        sync_reset_market_count = 0

        ranked_market_keys = sorted(
            (
                (market_key, delta)
                for market_key, delta in window.contributions.items()
                if abs(delta) > 1e-12
            ),
            key=lambda item: (-abs(item[1]), item[0]),
        )
        for market_key, reported_delta in ranked_market_keys:
            artifact = artifacts.get(market_key)
            if artifact is None:
                continue
            start_snapshot = equity_snapshot_at_or_before(
                artifact.equity_by_ts, window.start_ts_ms
            )
            end_snapshot = equity_snapshot_at_or_before(
                artifact.equity_by_ts, window.end_ts_ms
            )
            if start_snapshot is None or end_snapshot is None:
                continue

            active_market_ids = impacted_market_ids(
                artifact.trades,
                start_ts_ms=window.start_ts_ms,
                end_ts_ms=window.end_ts_ms,
            )
            context = context_provider(
                artifact.db_path,
                active_market_ids,
                window.start_ts_ms,
                window.end_ts_ms,
                padding_minutes,
            )
            breakdowns = build_market_minute_breakdown(
                trades=artifact.trades,
                market_specs=context.market_specs,
                price_series=context.price_series,
                cex_close_by_ts=context.cex_close_by_ts,
                stress=artifact.stress,
                start_ts_ms=window.start_ts_ms,
                end_ts_ms=window.end_ts_ms,
            )

            family_fill = sum(item.fill_cash_change_usd for item in breakdowns)
            family_mark = sum(
                item.poly_mark_change_usd + item.cex_mark_change_usd for item in breakdowns
            )
            family_entries = sum(item.entries_at_end_ts for item in breakdowns)
            family_exits = sum(item.exits_at_end_ts for item in breakdowns)
            family_equity_change = end_snapshot.equity - start_snapshot.equity
            family_cash_change = end_snapshot.cash - start_snapshot.cash

            top_contributors = []
            for item in breakdowns[:top_market_limit]:
                quote_path = build_quote_path(
                    market_id=item.market_id,
                    context=context,
                    start_ts_ms=window.start_ts_ms,
                    end_ts_ms=window.end_ts_ms,
                    padding_minutes=padding_minutes,
                )
                if is_sync_reset_like_contributor(
                    quote_path,
                    end_ts_ms=window.end_ts_ms,
                ):
                    sync_reset_market_count += 1
                top_contributors.append(
                    {
                        **asdict(item),
                        "quote_path": quote_path,
                    }
                )

            aggregate_fill += family_fill
            aggregate_mark += family_mark
            aggregate_cash_change_observed += family_cash_change
            aggregate_entries += family_entries
            aggregate_exits += family_exits
            aggregate_equity_change += family_equity_change
            minute_markets.append(
                {
                    "market": market_key,
                    "reported_delta_usd": reported_delta,
                    "equity_change_usd": family_equity_change,
                    "cash_change_usd": family_cash_change,
                    "fill_cash_change_usd": family_fill,
                    "mark_change_usd": family_mark,
                    "entries_at_end_ts": family_entries,
                    "exits_at_end_ts": family_exits,
                    "open_positions_before": start_snapshot.open_positions,
                    "open_positions_after": end_snapshot.open_positions,
                    "top_market_contributors": top_contributors,
                }
            )

        classification_fill = aggregate_fill
        classification_mark = aggregate_mark
        if abs(aggregate_equity_change - (aggregate_fill + aggregate_mark)) > 1e-6:
            classification_fill = aggregate_cash_change_observed
            classification_mark = aggregate_equity_change - aggregate_cash_change_observed

        classification = classify_minute_window(
            equity_change_usd=aggregate_equity_change or window.aggregate_delta_usd,
            fill_cash_change_usd=classification_fill,
            mark_change_usd=classification_mark,
            entries_at_end_ts=aggregate_entries,
            exits_at_end_ts=aggregate_exits,
            sync_reset_market_count=sync_reset_market_count,
        )
        minutes_payload.append(
            {
                "start_ts_ms": window.start_ts_ms,
                "end_ts_ms": window.end_ts_ms,
                "start_utc": window.start_utc,
                "end_utc": window.end_utc,
                "run_kind": window.run_kind,
                "aggregate_delta_usd_reported": window.aggregate_delta_usd,
                "aggregate_delta_usd_recomputed": aggregate_equity_change,
                "aggregate_fill_cash_change_usd": aggregate_fill,
                "aggregate_mark_change_usd": aggregate_mark,
                "aggregate_cash_change_usd_observed": aggregate_cash_change_observed,
                "aggregate_mark_change_usd_observed": (
                    aggregate_equity_change - aggregate_cash_change_observed
                ),
                "aggregate_entries_at_end_ts": aggregate_entries,
                "aggregate_exits_at_end_ts": aggregate_exits,
                "sync_reset_market_count": sync_reset_market_count,
                "classification": asdict(classification),
                "markets": minute_markets,
            }
        )

    return {"minutes": minutes_payload}


class DuckDbContextProvider:
    def __init__(self) -> None:
        self._db_cache: dict[str, tuple[Any, str, dict[str, MarketSpec]]] = {}

    def __call__(
        self,
        db_path: str,
        market_ids: list[str],
        start_ts_ms: int,
        end_ts_ms: int,
        padding_minutes: int,
    ) -> WindowContext:
        conn, cex_table, market_specs = self._open_cached(db_path)
        lower = start_ts_ms - padding_minutes * ONE_MINUTE_MS
        upper = end_ts_ms + padding_minutes * ONE_MINUTE_MS
        selected_market_specs = (
            {market_id: market_specs[market_id] for market_id in market_ids if market_id in market_specs}
            if market_ids
            else {}
        )
        price_series: dict[str, dict[int, PricePoint]] = {market_id: {} for market_id in selected_market_specs}
        if selected_market_specs:
            placeholders = ",".join(["?"] * len(selected_market_specs))
            sql = f"""
                with minute_market as (
                    select
                        market_id,
                        ts_ms,
                        max(case when lower(token_side) = 'yes' then poly_price end) as yes_price,
                        max(case when lower(token_side) = 'no' then poly_price end) as no_price
                    from polymarket_price_history
                    where market_id in ({placeholders}) and ts_ms between ? and ?
                    group by market_id, ts_ms
                )
                select market_id, ts_ms, yes_price, no_price
                from minute_market
                order by ts_ms asc, market_id asc
            """
            params: list[Any] = list(selected_market_specs) + [lower, upper]
            for market_id, ts_ms, yes_price, no_price in conn.execute(sql, params).fetchall():
                price_series.setdefault(str(market_id), {})[int(ts_ms)] = PricePoint(
                    yes_price=None if yes_price is None else float(yes_price),
                    no_price=None if no_price is None else float(no_price),
                )

        cex_close_by_ts = {
            int(ts_ms): float(price)
            for ts_ms, price in conn.execute(
                f"select ts_ms, close from {cex_table} where ts_ms between ? and ? order by ts_ms asc",
                [lower, upper],
            ).fetchall()
        }
        return WindowContext(
            market_specs=selected_market_specs,
            price_series=price_series,
            cex_close_by_ts=cex_close_by_ts,
        )

    def _open_cached(self, db_path: str) -> tuple[Any, str, dict[str, MarketSpec]]:
        cached = self._db_cache.get(db_path)
        if cached is not None:
            return cached
        if duckdb is None:
            raise RuntimeError("duckdb is required to read minute context")
        conn = duckdb.connect(db_path, read_only=True)
        metadata_rows = conn.execute(
            "select key, value from build_metadata order by key asc"
        ).fetchall()
        metadata = {str(key): str(value) for key, value in metadata_rows}
        cex_table = metadata.get("cex_table", "binance_btc_1m")
        market_specs = {
            str(market_id): MarketSpec(
                event_id=str(event_id),
                market_id=str(market_id),
                market_slug=f"mkt-{market_id}",
                market_question=market_question,
            )
            for market_id, event_id, market_question in conn.execute(
                """
                select
                    market_id,
                    any_value(event_id) as event_id,
                    any_value(market_question) as market_question
                from polymarket_contracts
                group by market_id
                order by market_id asc
                """
            ).fetchall()
        }
        cached = (conn, cex_table, market_specs)
        self._db_cache[db_path] = cached
        return cached


def write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_summary_csv(path: str, payload: dict[str, Any]) -> None:
    fieldnames = [
        "start_utc",
        "end_utc",
        "classification_code",
        "aggregate_delta_usd_reported",
        "aggregate_delta_usd_recomputed",
        "aggregate_fill_cash_change_usd",
        "aggregate_mark_change_usd",
        "aggregate_cash_change_usd_observed",
        "aggregate_mark_change_usd_observed",
        "aggregate_entries_at_end_ts",
        "aggregate_exits_at_end_ts",
        "sync_reset_market_count",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for minute in payload["minutes"]:
            writer.writerow(
                {
                    "start_utc": minute["start_utc"],
                    "end_utc": minute["end_utc"],
                    "classification_code": minute["classification"]["code"],
                    "aggregate_delta_usd_reported": minute["aggregate_delta_usd_reported"],
                    "aggregate_delta_usd_recomputed": minute["aggregate_delta_usd_recomputed"],
                    "aggregate_fill_cash_change_usd": minute["aggregate_fill_cash_change_usd"],
                    "aggregate_mark_change_usd": minute["aggregate_mark_change_usd"],
                    "aggregate_cash_change_usd_observed": minute[
                        "aggregate_cash_change_usd_observed"
                    ],
                    "aggregate_mark_change_usd_observed": minute[
                        "aggregate_mark_change_usd_observed"
                    ],
                    "aggregate_entries_at_end_ts": minute["aggregate_entries_at_end_ts"],
                    "aggregate_exits_at_end_ts": minute["aggregate_exits_at_end_ts"],
                    "sync_reset_market_count": minute["sync_reset_market_count"],
                }
            )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    os.makedirs(args.output_dir, exist_ok=True)
    top_minutes = load_top_minutes_csv(
        args.top_minutes_csv,
        run_kind=args.run_kind,
        limit=args.limit,
    )
    artifacts = discover_run_artifacts(args.run_dir)
    payload = build_triage_payload(
        top_minutes=top_minutes,
        artifacts=artifacts,
        context_provider=DuckDbContextProvider(),
        padding_minutes=args.padding_minutes,
        top_market_limit=args.top_market_limit,
    )
    write_json(os.path.join(args.output_dir, "top_minute_triage.json"), payload)
    write_summary_csv(os.path.join(args.output_dir, "top_minute_triage.csv"), payload)


if __name__ == "__main__":
    main()
