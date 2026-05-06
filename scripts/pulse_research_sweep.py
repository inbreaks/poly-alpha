from __future__ import annotations

import argparse
import heapq
import itertools
import json
import math
import subprocess
import tomllib
from array import array
from collections import deque
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "multi-market-active.fresh.toml"
DEFAULT_TAPE_DIR = ROOT / "data" / "server-downloads" / "2026-04-18-overnight" / "extracted" / "20260418"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "reports" / "pulse_research"


@dataclass(frozen=True)
class MarketRule:
    kind: str
    lower_strike: float | None
    upper_strike: float | None


@dataclass(frozen=True)
class MarketSpec:
    symbol: str
    asset: str
    settlement_ts_ms: int
    min_tick_size: float
    cex_price_tick: float
    cex_qty_step: float
    rule: MarketRule


@dataclass(frozen=True)
class RuntimeParams:
    opening_request_notional_usd: float
    min_expected_net_pnl_usd: float
    min_net_session_edge_bps: float
    pulse_window_ms: int
    min_claim_price_move_bps: float
    max_fair_claim_move_bps: float
    max_cex_mid_move_bps: float
    min_pulse_score_bps: float
    max_timeout_loss_usd: float
    max_required_hit_rate: float
    poly_fee_bps: float
    cex_taker_fee_bps: float
    cex_slippage_bps: float
    poly_open_max_quote_age_ms: int
    cex_open_max_quote_age_ms: int
    max_anchor_age_ms: int
    max_anchor_latency_delta_ms: int
    base_target_ticks: int
    medium_target_ticks: int
    strong_target_ticks: int
    medium_pulse_score_bps: float
    strong_pulse_score_bps: float


@dataclass(frozen=True)
class SurfacePoint:
    strike: float
    mark_iv: float


@dataclass(frozen=True)
class AnchorSnapshot:
    ts_ms: int
    index_price: float
    expiry_ts_ms: int
    atm_iv: float
    points: list[SurfacePoint]
    latest_received_at_ms: int


@dataclass(frozen=True)
class ExpiryGapAdjustment:
    grace_gap_hours: int
    soft_window_hours: int
    hard_cap_hours: int


@dataclass(frozen=True)
class SignalSample:
    ts_ms: int
    received_at_ms: int
    sequence: int
    value: float


@dataclass(frozen=True)
class SignalWindowSnapshot:
    first_value: float
    last_value: float
    min_value: float
    max_value: float
    min_ts_ms: int
    max_ts_ms: int
    sample_count: int


@dataclass(frozen=True)
class FlowReversionMetrics:
    window_displacement_bps: float
    residual_gap_bps: float
    rebound_from_low_bps: float
    refill_ratio: float
    extreme_age_ms: int


@dataclass(frozen=True)
class ConfirmationSample:
    ts_ms: int
    price: float
    spread_bps: float
    top3_bid_notional: float


@dataclass(frozen=True)
class ConfirmationMetrics:
    confirm_window_complete_1s: bool
    confirm_window_complete_3s: bool
    post_pulse_refill_ratio_1s: float | None
    post_pulse_refill_ratio_3s: float | None
    spread_snapback_bps: float | None
    bid_depth_rebuild_ratio: float | None


class SignalTape:
    def __init__(self) -> None:
        self._decimal_series: dict[str, deque[SignalSample]] = {
            "yes_ask": deque(),
            "no_ask": deque(),
            "cex_mid": deque(),
        }
        self._float_series: dict[str, deque[SignalSample]] = {
            "fair_prob_yes": deque(),
        }

    def push_decimal(
        self,
        series: str,
        *,
        ts_ms: int,
        received_at_ms: int,
        sequence: int,
        value: float,
        retention_ms: int,
    ) -> None:
        self._push(self._decimal_series[series], ts_ms, received_at_ms, sequence, value, retention_ms)

    def push_float(
        self,
        series: str,
        *,
        ts_ms: int,
        received_at_ms: int,
        sequence: int,
        value: float,
        retention_ms: int,
    ) -> None:
        self._push(self._float_series[series], ts_ms, received_at_ms, sequence, value, retention_ms)

    def latest_decimal_before(self, series: str, before_ts_ms: int, window_ms: int) -> float | None:
        return self._latest_before(self._decimal_series[series], before_ts_ms, window_ms)

    def latest_float_before(self, series: str, before_ts_ms: int, window_ms: int) -> float | None:
        return self._latest_before(self._float_series[series], before_ts_ms, window_ms)

    def window_decimal_snapshot(self, series: str, before_ts_ms: int, window_ms: int) -> SignalWindowSnapshot | None:
        return self._window_snapshot(self._decimal_series[series], before_ts_ms, window_ms)

    def window_float_snapshot(self, series: str, before_ts_ms: int, window_ms: int) -> SignalWindowSnapshot | None:
        return self._window_snapshot(self._float_series[series], before_ts_ms, window_ms)

    @staticmethod
    def _push(
        samples: deque[SignalSample],
        ts_ms: int,
        received_at_ms: int,
        sequence: int,
        value: float,
        retention_ms: int,
    ) -> None:
        samples.append(SignalSample(ts_ms=ts_ms, received_at_ms=received_at_ms, sequence=sequence, value=value))
        min_ts = ts_ms - max(retention_ms, 0)
        while samples and samples[0].ts_ms < min_ts:
            samples.popleft()

    @staticmethod
    def _latest_before(samples: deque[SignalSample], before_ts_ms: int, window_ms: int) -> float | None:
        min_ts = before_ts_ms - window_ms
        for sample in reversed(samples):
            if min_ts <= sample.ts_ms < before_ts_ms:
                return sample.value
        return None

    @staticmethod
    def _window_snapshot(samples: deque[SignalSample], before_ts_ms: int, window_ms: int) -> SignalWindowSnapshot | None:
        min_ts = before_ts_ms - window_ms
        selected = [sample for sample in samples if min_ts <= sample.ts_ms < before_ts_ms]
        if not selected:
            return None
        min_sample = min(selected, key=lambda item: item.value)
        max_sample = max(selected, key=lambda item: item.value)
        return SignalWindowSnapshot(
            first_value=selected[0].value,
            last_value=selected[-1].value,
            min_value=min_sample.value,
            max_value=max_sample.value,
            min_ts_ms=min_sample.ts_ms,
            max_ts_ms=max_sample.ts_ms,
            sample_count=len(selected),
        )


class ConfirmationTape:
    def __init__(self) -> None:
        self._samples: deque[ConfirmationSample] = deque()

    def push(self, sample: ConfirmationSample, *, retention_ms: int) -> None:
        self._samples.append(sample)
        min_ts_ms = sample.ts_ms - max(retention_ms, 0)
        while self._samples and self._samples[0].ts_ms < min_ts_ms:
            self._samples.popleft()

    def recent_samples(self) -> list[ConfirmationSample]:
        return list(self._samples)


@dataclass
class OrderBook:
    bids: dict[float, float]
    asks: dict[float, float]
    ts_ms: int
    received_at_ms: int
    sequence: int

    def apply(self, bids: list[list[str]], asks: list[list[str]], record_kind: str | None) -> None:
        if record_kind == "snapshot":
            self.bids = {}
            self.asks = {}
        for price_text, qty_text in bids:
            self._apply_level(self.bids, price_text, qty_text)
        for price_text, qty_text in asks:
            self._apply_level(self.asks, price_text, qty_text)

    @staticmethod
    def _apply_level(levels: dict[float, float], price_text: str, qty_text: str) -> None:
        price = float(price_text)
        qty = float(qty_text)
        if qty <= 0.0:
            levels.pop(price, None)
        else:
            levels[price] = qty

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def mid(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2.0

    def top_bid_notional(self, levels: int = 3) -> float:
        total = 0.0
        for price in sorted(self.bids, reverse=True)[: max(levels, 0)]:
            qty = self.bids[price]
            if qty <= 0.0:
                continue
            total += price * qty
        return total

    def executable_buy_notional(self, requested_notional_usd: float) -> dict[str, float]:
        remaining = requested_notional_usd
        filled_notional = 0.0
        filled_shares = 0.0
        levels_consumed = 0
        top_price = self.best_ask()
        for price in sorted(self.asks):
            if remaining <= 0.0:
                break
            qty = self.asks[price]
            level_notional = price * qty
            if level_notional <= 0.0:
                continue
            take_notional = min(level_notional, remaining)
            filled_notional += take_notional
            filled_shares += take_notional / price
            remaining -= take_notional
            levels_consumed += 1
        avg_price = filled_notional / filled_shares if filled_shares > 0.0 else 0.0
        depth_fill_ratio = filled_notional / requested_notional_usd if requested_notional_usd > 0.0 else 0.0
        slippage_bps = 0.0
        if top_price and top_price > 0.0 and avg_price > 0.0:
            slippage_bps = ((avg_price - top_price) / top_price) * 10_000.0
        return {
            "requested_notional_usd": requested_notional_usd,
            "filled_notional_usd": filled_notional,
            "filled_shares": filled_shares,
            "avg_price": avg_price,
            "depth_shortfall_usd": max(remaining, 0.0),
            "depth_fill_ratio": max(0.0, min(depth_fill_ratio, 1.0)),
            "slippage_bps": slippage_bps,
            "levels_consumed": levels_consumed,
        }

    def executable_sell_shares(self, requested_shares: float) -> float | None:
        remaining = requested_shares
        filled_shares = 0.0
        filled_notional = 0.0
        for price in sorted(self.bids, reverse=True):
            if remaining <= 0.0:
                break
            qty = self.bids[price]
            if qty <= 0.0:
                continue
            take = min(qty, remaining)
            filled_shares += take
            filled_notional += take * price
            remaining -= take
        if filled_shares <= 0.0:
            return None
        return filled_notional / filled_shares


@dataclass
class CandidateRecord:
    ts_ms: int
    asset: str
    symbol: str
    signal_mode: str
    claim_side: str
    entry_price: float
    fair_prob_yes: float
    fair_claim_price: float
    instant_basis_bps: float
    claim_price_move_bps: float
    window_displacement_bps: float
    refill_ratio: float
    extreme_age_ms: int
    fair_claim_move_bps: float
    cex_mid_move_bps: float
    pulse_score_bps: float
    pocket_ticks: float
    target_exit_price: float
    timeout_exit_price: float
    expected_net_edge_bps: float
    expected_net_pnl_usd: float
    timeout_net_pnl_usd: float
    timeout_loss_estimate_usd: float
    target_distance_to_mid_bps: float
    required_hit_rate: float
    predicted_hit_rate: float
    realizable_ev_usd: float
    levels_consumed: int
    anchor_age_ms: int
    anchor_latency_delta_ms: int


@dataclass(frozen=True)
class ResearchFeatureRow:
    ts_ms: int
    asset: str
    symbol: str
    signal_mode: str
    claim_side: str
    entry_price: float
    current_bid: float
    current_ask: float
    current_mid: float
    filled_notional_usd: float
    filled_shares: float
    levels_consumed: int
    depth_fill_ratio: float
    min_tick_size: float
    fair_prob_yes: float
    fair_claim_price: float
    anchor_age_ms: int
    anchor_latency_delta_ms: int
    window_first_price: float
    window_last_price: float
    window_high_price: float
    window_low_price: float
    window_high_ts_ms: int
    window_low_ts_ms: int
    claim_move_bps: float
    window_displacement_bps: float
    flow_refill_ratio: float
    flow_rebound_bps: float
    flow_residual_gap_bps: float
    flow_extreme_age_ms: int
    flow_shape_valid: bool
    fair_claim_move_bps: float
    cex_mid_move_bps: float
    timeout_exit_price: float
    residual_basis_bps: float = 0.0
    anchor_follow_ratio: float = 0.0
    market_rule_kind: str = "unknown"
    market_family: str = "unknown"
    entry_price_bucket: str = "unknown"
    time_to_settlement_hours: float = 0.0
    time_to_settlement_bucket: str = "unknown"
    post_pulse_refill_ratio_1s: float | None = None
    post_pulse_refill_ratio_3s: float | None = None
    spread_snapback_bps: float | None = None
    bid_depth_rebuild_ratio: float | None = None
    confirm_window_complete_1s: bool = False
    confirm_window_complete_3s: bool = False


@dataclass(frozen=True)
class HorizonPriceOutcome:
    future_max_bid: list[float]
    future_max_touch: list[float]
    deadline_bid: list[float]


@dataclass(frozen=True)
class AggressiveExitConfig:
    horizon_label: str
    target_ticks: int
    touch_mode: str
    target_mode: str


@dataclass
class SidePriceSeries:
    times_ms: array
    best_bids: array
    last_trades: array

    @classmethod
    def empty(cls) -> "SidePriceSeries":
        return cls(times_ms=array("Q"), best_bids=array("d"), last_trades=array("d"))

    def append(self, ts_ms: int, best_bid: float, last_trade: float) -> None:
        self.times_ms.append(max(ts_ms, 0))
        self.best_bids.append(best_bid)
        self.last_trades.append(last_trade)



def infer_asset(symbol: str) -> str:
    if "bitcoin" in symbol:
        return "btc"
    if "ethereum" in symbol:
        return "eth"
    raise ValueError(f"unsupported asset in symbol `{symbol}`")


def load_market_catalog(path: Path) -> dict[str, MarketSpec]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    markets = raw.get("markets", [])
    catalog: dict[str, MarketSpec] = {}
    for item in markets:
        symbol = item["symbol"]
        rule_raw = item.get("market_rule")
        if not rule_raw:
            continue
        rule = MarketRule(
            kind=str(rule_raw["kind"]),
            lower_strike=_parse_optional_float(rule_raw.get("lower_strike")),
            upper_strike=_parse_optional_float(rule_raw.get("upper_strike")),
        )
        catalog[symbol] = MarketSpec(
            symbol=symbol,
            asset=infer_asset(symbol),
            settlement_ts_ms=int(item["settlement_timestamp"]) * 1000,
            min_tick_size=float(item["min_tick_size"]),
            cex_price_tick=float(item["cex_price_tick"]),
            cex_qty_step=float(item["cex_qty_step"]),
            rule=rule,
        )
    return catalog


def load_runtime_params(path: Path) -> RuntimeParams:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    pulse = raw["strategy"]["pulse_arb"]
    entry = pulse["entry"]
    session = pulse["session"]
    exit_cfg = pulse["exit"]
    providers = pulse["providers"]
    deribit = providers["deribit_primary"]
    return RuntimeParams(
        opening_request_notional_usd=float(session["opening_request_notional_usd"]),
        min_expected_net_pnl_usd=float(session["min_expected_net_pnl_usd"]),
        min_net_session_edge_bps=float(entry["min_net_session_edge_bps"]),
        pulse_window_ms=int(entry["pulse_window_ms"]),
        min_claim_price_move_bps=float(entry["min_claim_price_move_bps"]),
        max_fair_claim_move_bps=float(entry["max_fair_claim_move_bps"]),
        max_cex_mid_move_bps=float(entry["max_cex_mid_move_bps"]),
        min_pulse_score_bps=float(entry["min_pulse_score_bps"]),
        max_timeout_loss_usd=float(entry.get("max_timeout_loss_usd", 20.0)),
        max_required_hit_rate=float(entry.get("max_required_hit_rate", 0.70)),
        poly_fee_bps=float(raw["execution_costs"]["poly_fee_bps"]),
        cex_taker_fee_bps=float(raw["execution_costs"]["cex_taker_fee_bps"]),
        cex_slippage_bps=float(raw["paper_slippage"]["cex_slippage_bps"]),
        poly_open_max_quote_age_ms=int(raw["strategy"]["market_data"]["poly_open_max_quote_age_ms"]),
        cex_open_max_quote_age_ms=int(raw["strategy"]["market_data"]["cex_open_max_quote_age_ms"]),
        max_anchor_age_ms=int(deribit["max_anchor_age_ms"]),
        max_anchor_latency_delta_ms=int(deribit["max_anchor_latency_delta_ms"]),
        base_target_ticks=int(exit_cfg["base_target_ticks"]),
        medium_target_ticks=int(exit_cfg["medium_target_ticks"]),
        strong_target_ticks=int(exit_cfg["strong_target_ticks"]),
        medium_pulse_score_bps=float(exit_cfg["medium_pulse_score_bps"]),
        strong_pulse_score_bps=float(exit_cfg["strong_pulse_score_bps"]),
    )


def _parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def interpolated_mark_iv(anchor: AnchorSnapshot, target_strike: float) -> float | None:
    points = sorted(anchor.points, key=lambda item: item.strike)
    if not points:
        return None
    unique: list[SurfacePoint] = []
    for point in points:
        if unique and math.isclose(unique[-1].strike, point.strike, rel_tol=0.0, abs_tol=1e-9):
            prev = unique[-1]
            unique[-1] = SurfacePoint(strike=prev.strike, mark_iv=(prev.mark_iv + point.mark_iv) / 2.0)
            continue
        unique.append(point)
    if target_strike <= unique[0].strike:
        return unique[0].mark_iv
    if target_strike >= unique[-1].strike:
        return unique[-1].mark_iv
    for left, right in zip(unique, unique[1:]):
        if left.strike <= target_strike <= right.strike:
            span = right.strike - left.strike
            if math.isclose(span, 0.0, rel_tol=0.0, abs_tol=1e-12):
                return (left.mark_iv + right.mark_iv) / 2.0
            weight = (target_strike - left.strike) / span
            return left.mark_iv + (right.mark_iv - left.mark_iv) * weight
    return unique[-1].mark_iv


def probability_for_rule(
    *,
    anchor: AnchorSnapshot,
    rule: MarketRule,
    event_expiry_ts_ms: int,
    expiry_gap: ExpiryGapAdjustment,
) -> float:
    option_t_years = time_to_expiry_years(anchor.ts_ms, anchor.expiry_ts_ms)
    event_t_years = time_to_expiry_years(anchor.ts_ms, event_expiry_ts_ms)
    unadjusted = _probability_for_rule_time(anchor, rule, anchor.index_price, option_t_years)
    bridged = _probability_for_rule_time(anchor, rule, anchor.index_price, event_t_years)
    return apply_expiry_gap_adjustment(
        unadjusted_prob=unadjusted,
        bridged_target_prob=bridged,
        option_t_years=option_t_years,
        event_t_years=event_t_years,
        adjustment=expiry_gap,
    )


def _probability_for_rule_time(anchor: AnchorSnapshot, rule: MarketRule, index_price: float, time_to_expiry_years: float) -> float:
    if rule.kind == "above":
        return probability_above(anchor, index_price, rule.lower_strike or 0.0, time_to_expiry_years)
    if rule.kind == "below":
        return clamp_probability(1.0 - probability_above(anchor, index_price, rule.upper_strike or 0.0, time_to_expiry_years))
    if rule.kind == "between":
        lower = probability_above(anchor, index_price, rule.lower_strike or 0.0, time_to_expiry_years)
        upper = probability_above(anchor, index_price, rule.upper_strike or 0.0, time_to_expiry_years)
        return clamp_probability(lower - upper)
    raise ValueError(f"unsupported rule kind `{rule.kind}`")


def probability_above(anchor: AnchorSnapshot, index_price: float, strike: float, time_to_expiry_years: float) -> float:
    if not math.isfinite(index_price) or index_price <= 0.0 or not math.isfinite(strike) or strike <= 0.0:
        return 0.0
    iv = (interpolated_mark_iv(anchor, strike) or anchor.atm_iv) / 100.0
    if time_to_expiry_years <= 0.0 or iv <= 0.0:
        if index_price > strike:
            return 1.0
        if index_price < strike:
            return 0.0
        return 0.5
    sigma_sqrt_t = iv * math.sqrt(time_to_expiry_years)
    if sigma_sqrt_t <= 1e-12:
        if index_price > strike:
            return 1.0
        if index_price < strike:
            return 0.0
        return 0.5
    d2 = (math.log(index_price / strike) - 0.5 * iv * iv * time_to_expiry_years) / sigma_sqrt_t
    return clamp_probability(standard_normal_cdf(d2))


def apply_expiry_gap_adjustment(
    *,
    unadjusted_prob: float,
    bridged_target_prob: float,
    option_t_years: float,
    event_t_years: float,
    adjustment: ExpiryGapAdjustment,
) -> float:
    gap_hours = abs(event_t_years - option_t_years) * 365.0 * 24.0
    if gap_hours > adjustment.hard_cap_hours:
        raise ValueError(f"hard expiry gap exceeded: {gap_hours:.2f}h > {adjustment.hard_cap_hours}h")
    if gap_hours <= adjustment.grace_gap_hours:
        return clamp_probability(unadjusted_prob)
    base = max(adjustment.soft_window_hours - adjustment.grace_gap_hours, 1)
    effective = min(gap_hours, adjustment.soft_window_hours)
    blend = max(0.0, min(1.0, (effective - adjustment.grace_gap_hours) / base))
    return clamp_probability(unadjusted_prob + (bridged_target_prob - unadjusted_prob) * blend)


def time_to_expiry_years(reference_ts_ms: int, expiry_ts_ms: int) -> float:
    millis_per_year = 365.0 * 24.0 * 60.0 * 60.0 * 1000.0
    return max(expiry_ts_ms - reference_ts_ms, 0) / millis_per_year


def standard_normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def clamp_probability(value: float) -> float:
    return max(0.0, min(1.0, value))


def compute_flow_reversion_metrics(
    *,
    window: SignalWindowSnapshot,
    current_value: float,
    now_ms: int,
) -> FlowReversionMetrics | None:
    if window.sample_count < 2:
        return None
    if not math.isfinite(current_value) or current_value <= 0.0:
        return None
    if window.max_value <= window.min_value:
        return None
    if window.max_ts_ms >= window.min_ts_ms:
        return None
    if current_value <= window.min_value or current_value >= window.max_value:
        return None

    sweep_span = window.max_value - window.min_value
    refill_ratio = (current_value - window.min_value) / sweep_span
    return FlowReversionMetrics(
        window_displacement_bps=((window.max_value - window.min_value) / window.max_value) * 10_000.0,
        residual_gap_bps=((window.max_value - current_value) / window.max_value) * 10_000.0,
        rebound_from_low_bps=((current_value - window.min_value) / window.min_value) * 10_000.0
        if window.min_value > 0.0
        else 0.0,
        refill_ratio=max(0.0, min(refill_ratio, 1.0)),
        extreme_age_ms=max(now_ms - window.min_ts_ms, 0),
    )


def _first_confirmation_sample_at_or_after(samples: list[ConfirmationSample], target_ts_ms: int) -> ConfirmationSample | None:
    for sample in samples:
        if sample.ts_ms >= target_ts_ms:
            return sample
    return None


def compute_post_pulse_confirmation_metrics(
    *,
    samples: list[ConfirmationSample],
    window_high_price: float,
    window_low_price: float,
    window_low_ts_ms: int,
    now_ts_ms: int,
) -> ConfirmationMetrics | None:
    if not samples:
        return None
    if not math.isfinite(window_high_price) or not math.isfinite(window_low_price):
        return None
    if window_high_price <= window_low_price:
        return None

    anchor_sample = _first_confirmation_sample_at_or_after(samples, window_low_ts_ms)
    if anchor_sample is None:
        return None

    sweep_span = window_high_price - window_low_price
    if sweep_span <= 0.0:
        return None

    one_second_ready = now_ts_ms >= window_low_ts_ms + 1_000
    three_second_ready = now_ts_ms >= window_low_ts_ms + 3_000
    one_second_sample = _first_confirmation_sample_at_or_after(samples, window_low_ts_ms + 1_000) if one_second_ready else None
    three_second_sample = _first_confirmation_sample_at_or_after(samples, window_low_ts_ms + 3_000) if three_second_ready else None

    def _refill_ratio(sample: ConfirmationSample | None) -> float | None:
        if sample is None:
            return None
        refill_ratio = (sample.price - window_low_price) / sweep_span
        return max(0.0, min(refill_ratio, 1.0))

    spread_snapback_bps = None
    bid_depth_rebuild_ratio = None
    if three_second_sample is not None:
        spread_snapback_bps = anchor_sample.spread_bps - three_second_sample.spread_bps
        depth_base = anchor_sample.top3_bid_notional if anchor_sample.top3_bid_notional > 0.0 else 1e-9
        bid_depth_rebuild_ratio = three_second_sample.top3_bid_notional / depth_base

    return ConfirmationMetrics(
        confirm_window_complete_1s=one_second_sample is not None,
        confirm_window_complete_3s=three_second_sample is not None,
        post_pulse_refill_ratio_1s=_refill_ratio(one_second_sample),
        post_pulse_refill_ratio_3s=_refill_ratio(three_second_sample),
        spread_snapback_bps=spread_snapback_bps,
        bid_depth_rebuild_ratio=bid_depth_rebuild_ratio,
    )


def range_move_bps(window: SignalWindowSnapshot) -> float:
    reference = window.first_value if window.first_value > 0.0 else window.max_value
    if reference <= 0.0:
        return 0.0
    return abs(window.max_value - window.min_value) / reference * 10_000.0


def residual_basis_bps(*, entry_price: float, fair_claim_price: float) -> float:
    if not math.isfinite(entry_price) or entry_price <= 0.0:
        return 0.0
    return ((fair_claim_price - entry_price) / entry_price) * 10_000.0


def anchor_follow_ratio(*, window_displacement_bps: float, fair_claim_move_bps: float) -> float:
    if not math.isfinite(window_displacement_bps) or window_displacement_bps <= 0.0:
        return 0.0 if fair_claim_move_bps <= 0.0 else float("inf")
    return max(fair_claim_move_bps, 0.0) / window_displacement_bps


def market_family_for_rule(rule_kind: str) -> str:
    if rule_kind in {"above", "below"}:
        return "directional"
    if rule_kind == "between":
        return "range"
    return "unknown"


def entry_price_bucket(entry_price: float) -> str:
    if entry_price <= 0.15:
        return "low"
    if entry_price <= 0.50:
        return "mid"
    return "high"


def time_to_settlement_bucket(hours_to_settlement: float) -> str:
    if hours_to_settlement < 6.0:
        return "lt6h"
    if hours_to_settlement < 24.0:
        return "6h_to_24h"
    if hours_to_settlement < 72.0:
        return "24h_to_72h"
    return "gt72h"


def iter_jsonl_zst(path: Path) -> Iterable[dict[str, Any]]:
    proc = subprocess.Popen(
        ["zstd", "-dc", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    pending_line: str | None = None
    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            if pending_line is not None:
                yield json.loads(pending_line)
            pending_line = line
    finally:
        if proc.stdout:
            proc.stdout.close()
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        if proc.stderr is not None:
            proc.stderr.close()
        code = proc.wait()
        normalized_stderr = stderr.strip().lower()
        if pending_line is not None:
            try:
                yield json.loads(pending_line)
            except json.JSONDecodeError:
                truncated_terminal_line = (
                    code != 0 and "premature end" in normalized_stderr
                ) or not pending_line.endswith("}")
                if not truncated_terminal_line:
                    raise
        if code != 0 and normalized_stderr and "premature end" not in normalized_stderr:
            raise RuntimeError(f"failed to read {path}: {stderr.strip()}")


def merge_event_streams(tape_dir: Path) -> Iterable[dict[str, Any]]:
    prefixes = ["poly_book", "binance_book", "deribit_option_ticker"]
    stream_iters: list[Iterable[dict[str, Any]]] = []
    for prefix in prefixes:
        paths = sorted(tape_dir.glob(f"{prefix}-*.jsonl.zst"))
        stream_iters.append(_iter_stream_paths(paths))
    iterators = [iter(stream) for stream in stream_iters]
    heap: list[tuple[int, int, dict[str, Any]]] = []
    for idx, iterator in enumerate(iterators):
        try:
            item = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (int(item["ts_recv_ms"]), idx, item))
    while heap:
        _, idx, item = heapq.heappop(heap)
        yield item
        try:
            nxt = next(iterators[idx])
        except StopIteration:
            continue
        heapq.heappush(heap, (int(nxt["ts_recv_ms"]), idx, nxt))


def _iter_stream_paths(paths: list[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        yield from iter_jsonl_zst(path)


def write_checkpoint(output_dir: Path, stage: str, label: str, payload: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = output_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    checkpoint_path = checkpoints_dir / f"{stage}-{label}-{now}.json"
    checkpoint_payload = {
        "stage": stage,
        "label": label,
        "generated_at_utc": now,
        "payload": payload,
    }
    checkpoint_path.write_text(
        json.dumps(checkpoint_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {"latest": {}, "history": []}
    manifest.setdefault("latest", {})[stage] = checkpoint_path.name
    manifest.setdefault("history", []).append(
        {
            "stage": stage,
            "label": label,
            "path": checkpoint_path.name,
            "generated_at_utc": now,
        }
    )
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return checkpoint_path


def _frame_column(frame: pd.DataFrame, name: str, default: float | int | str | bool) -> pd.Series:
    if name in frame.columns:
        return frame[name]
    return pd.Series([default] * len(frame), index=frame.index)


def enrich_frame_with_market_context(frame: pd.DataFrame, catalog: dict[str, MarketSpec]) -> pd.DataFrame:
    enriched = frame.copy()
    if enriched.empty:
        return enriched

    symbol_series = _frame_column(enriched, "symbol", "")
    ts_ms_series = pd.to_numeric(_frame_column(enriched, "ts_ms", 0), errors="coerce").fillna(0).astype(float)
    entry_series = pd.to_numeric(_frame_column(enriched, "entry_price", 0.0), errors="coerce").fillna(0.0)
    fair_series = pd.to_numeric(_frame_column(enriched, "fair_claim_price", 0.0), errors="coerce").fillna(0.0)
    displacement_series = pd.to_numeric(_frame_column(enriched, "window_displacement_bps", 0.0), errors="coerce").fillna(0.0)
    fair_move_series = pd.to_numeric(_frame_column(enriched, "fair_claim_move_bps", 0.0), errors="coerce").fillna(0.0)

    if "residual_basis_bps" not in enriched.columns:
        residual_values = [
            residual_basis_bps(entry_price=float(entry), fair_claim_price=float(fair))
            for entry, fair in zip(entry_series, fair_series, strict=False)
        ]
        enriched["residual_basis_bps"] = residual_values

    if "anchor_follow_ratio" not in enriched.columns:
        follow_values = [
            anchor_follow_ratio(window_displacement_bps=float(disp), fair_claim_move_bps=float(fair_move))
            for disp, fair_move in zip(displacement_series, fair_move_series, strict=False)
        ]
        enriched["anchor_follow_ratio"] = follow_values

    if "market_rule_kind" not in enriched.columns:
        enriched["market_rule_kind"] = [catalog.get(str(symbol)).rule.kind if str(symbol) in catalog else "unknown" for symbol in symbol_series]

    if "market_family" not in enriched.columns:
        enriched["market_family"] = [market_family_for_rule(str(kind)) for kind in enriched["market_rule_kind"]]

    if "entry_price_bucket" not in enriched.columns:
        enriched["entry_price_bucket"] = [entry_price_bucket(float(entry)) for entry in entry_series]

    if "time_to_settlement_hours" not in enriched.columns:
        hours_values: list[float] = []
        for symbol, ts_ms in zip(symbol_series, ts_ms_series, strict=False):
            market = catalog.get(str(symbol))
            if market is None:
                hours_values.append(0.0)
                continue
            hours_values.append(max(market.settlement_ts_ms - float(ts_ms), 0.0) / (60.0 * 60.0 * 1000.0))
        enriched["time_to_settlement_hours"] = hours_values

    if "time_to_settlement_bucket" not in enriched.columns:
        enriched["time_to_settlement_bucket"] = [
            time_to_settlement_bucket(float(hours))
            for hours in pd.to_numeric(enriched["time_to_settlement_hours"], errors="coerce").fillna(0.0)
        ]

    return enriched


def analyze_signal_coarse(
    *,
    tape_dir: Path,
    params: RuntimeParams,
    config_path: Path,
    output_dir: Path,
    signal_mode: str = "baseline",
    checkpoint_every_poly_events: int = 250_000,
    label_prefix: str = "progress",
) -> Path:
    catalog = load_market_catalog(config_path)
    market_tapes: dict[str, SignalTape] = {}
    asset_cex_tapes: dict[str, SignalTape] = {"btc": SignalTape(), "eth": SignalTape()}
    books: dict[tuple[str, str], OrderBook] = {}
    cex_books: dict[str, OrderBook] = {}
    deribit_surface: dict[str, dict[int, dict[str, dict[str, Any]]]] = {"btc": {}, "eth": {}}
    candidates: list[CandidateRecord] = []
    stats = {
        "events_total": 0,
        "poly_events": 0,
        "binance_events": 0,
        "deribit_events": 0,
        "markets_seen": set(),
        "candidate_count": 0,
        "candidate_by_asset": {"btc": 0, "eth": 0},
        "candidate_by_side": {"yes": 0, "no": 0},
        "rejections": {},
    }

    for row in merge_event_streams(tape_dir):
        stats["events_total"] += 1
        stream = row["stream"]
        if stream == "deribit_option_ticker":
            stats["deribit_events"] += 1
            _update_deribit_surface(deribit_surface, row)
            continue
        if stream == "binance_book":
            stats["binance_events"] += 1
            asset = str(row["asset"])
            book = OrderBook(
                bids={},
                asks={},
                ts_ms=int(row["ts_exchange_ms"]),
                received_at_ms=int(row["ts_recv_ms"]),
                sequence=int(row["sequence"] or 0),
            )
            payload = row["payload"]
            book.apply(payload.get("bids", []), payload.get("asks", []), row.get("record_kind"))
            cex_books[asset] = book
            mid = book.mid()
            if mid is not None:
                asset_cex_tapes.setdefault(asset, SignalTape()).push_decimal(
                    "cex_mid",
                    ts_ms=book.ts_ms,
                    received_at_ms=book.received_at_ms,
                    sequence=book.sequence,
                    value=mid,
                    retention_ms=params.pulse_window_ms * 4,
                )
            continue
        if stream != "poly_book":
            continue

        stats["poly_events"] += 1
        symbol = str(row["symbol"])
        stats["markets_seen"].add(symbol)
        side = "yes" if row["token_side"] == "yes" else "no"
        key = (symbol, side)
        book = books.get(key)
        if book is None:
            book = OrderBook(bids={}, asks={}, ts_ms=0, received_at_ms=0, sequence=0)
            books[key] = book
        book.ts_ms = int(row["ts_exchange_ms"])
        book.received_at_ms = int(row["ts_recv_ms"])
        book.sequence = int(row["sequence"] or 0)
        payload = row["payload"]
        book.apply(payload.get("bids", []), payload.get("asks", []), row.get("record_kind"))

        tape = market_tapes.setdefault(symbol, SignalTape())
        ask = book.best_ask()
        if ask is not None:
            tape.push_decimal(
                f"{side}_ask",
                ts_ms=book.ts_ms,
                received_at_ms=book.received_at_ms,
                sequence=book.sequence,
                value=ask,
                retention_ms=params.pulse_window_ms * 4,
            )

        market = catalog.get(symbol)
        if market is None:
            continue
        asset = market.asset
        yes_book = books.get((symbol, "yes"))
        no_book = books.get((symbol, "no"))
        cex_book = cex_books.get(asset)
        if yes_book is None or no_book is None or cex_book is None:
            continue

        anchor = _anchor_snapshot_for_market(deribit_surface.get(asset, {}), market)
        if anchor is None:
            continue
        now_ms = int(row["ts_recv_ms"])
        try:
            fair_prob_yes = probability_for_rule(
                anchor=anchor,
                rule=market.rule,
                event_expiry_ts_ms=market.settlement_ts_ms,
                expiry_gap=ExpiryGapAdjustment(60, 360, 720),
            )
        except ValueError:
            _bump_reason(stats["rejections"], "hard_expiry_gap_exceeded")
            continue
        tape.push_float(
            "fair_prob_yes",
            ts_ms=now_ms,
            received_at_ms=now_ms,
            sequence=1,
            value=fair_prob_yes,
            retention_ms=params.pulse_window_ms * 4,
        )

        for claim_side, claim_book in (("yes", yes_book), ("no", no_book)):
            candidate, reason = _evaluate_claim_candidate(
                params=params,
                market=market,
                claim_side=claim_side,
                claim_book=claim_book,
                cex_book=cex_book,
                market_tape=tape,
                asset_cex_tape=asset_cex_tapes[asset],
                fair_prob_yes=fair_prob_yes,
                anchor=anchor,
                now_ms=now_ms,
                signal_mode=signal_mode,
            )
            if reason is not None:
                _bump_reason(stats["rejections"], reason)
            if candidate is None:
                continue
            candidates.append(candidate)
            stats["candidate_count"] += 1
            stats["candidate_by_asset"][asset] += 1
            stats["candidate_by_side"][claim_side] += 1

        if checkpoint_every_poly_events > 0 and stats["poly_events"] % checkpoint_every_poly_events == 0:
            _write_signal_summary_checkpoint(
                output_dir,
                stats,
                candidates[-25:],
                signal_mode=signal_mode,
                label=f"{label_prefix}-progress",
            )

    return _write_signal_summary_checkpoint(
        output_dir,
        stats,
        candidates[-50:],
        signal_mode=signal_mode,
        label=f"{label_prefix}-final",
    )


def _update_deribit_surface(surface: dict[str, dict[int, dict[str, dict[str, Any]]]], row: dict[str, Any]) -> None:
    asset = str(row["asset"])
    payload = row["payload"]
    expiry = int(payload["expiry_ts_ms"])
    instrument = str(row["instrument"])
    surface.setdefault(asset, {}).setdefault(expiry, {})[instrument] = {
        "strike": float(payload["strike"]),
        "mark_iv": float(payload["mark_iv"]),
        "index_price": float(payload["index_price"]),
        "ts_ms": int(row["ts_exchange_ms"]),
        "received_at_ms": int(row["ts_recv_ms"]),
    }


def _anchor_snapshot_for_market(expiries: dict[int, dict[str, dict[str, Any]]], market: MarketSpec) -> AnchorSnapshot | None:
    if not expiries:
        return None
    selected_expiry = min(expiries, key=lambda expiry: abs(expiry - market.settlement_ts_ms))
    points_raw = list(expiries[selected_expiry].values())
    if not points_raw:
        return None
    index_price = next((item["index_price"] for item in points_raw if math.isfinite(item["index_price"])), None)
    if index_price is None:
        return None
    latest_received = max(int(item["received_at_ms"]) for item in points_raw)
    ts_ms = max(int(item["ts_ms"]) for item in points_raw)
    points = [SurfacePoint(strike=float(item["strike"]), mark_iv=float(item["mark_iv"])) for item in points_raw]
    atm_point = min(points, key=lambda item: abs(item.strike - index_price))
    return AnchorSnapshot(
        ts_ms=ts_ms,
        index_price=float(index_price),
        expiry_ts_ms=int(selected_expiry),
        atm_iv=float(atm_point.mark_iv),
        points=points,
        latest_received_at_ms=latest_received,
    )


def _evaluate_claim_candidate(
    *,
    params: RuntimeParams,
    market: MarketSpec,
    claim_side: str,
    claim_book: OrderBook,
    cex_book: OrderBook,
    market_tape: SignalTape,
    asset_cex_tape: SignalTape,
    fair_prob_yes: float,
    anchor: AnchorSnapshot,
    now_ms: int,
    signal_mode: str = "baseline",
) -> tuple[CandidateRecord | None, str | None]:
    if now_ms - claim_book.received_at_ms > params.poly_open_max_quote_age_ms:
        return None, "poly_quote_stale"
    if now_ms - cex_book.received_at_ms > params.cex_open_max_quote_age_ms:
        return None, "cex_quote_stale"
    anchor_age_ms = now_ms - anchor.latest_received_at_ms
    anchor_latency_delta_ms = now_ms - anchor.ts_ms
    if anchor_age_ms > params.max_anchor_age_ms:
        return None, "anchor_age_rejected"
    if anchor_latency_delta_ms > params.max_anchor_latency_delta_ms:
        return None, "anchor_latency_delta_rejected"

    entry = claim_book.executable_buy_notional(params.opening_request_notional_usd)
    if entry["filled_notional_usd"] <= 0.0 or entry["avg_price"] <= 0.0:
        return None, "no_entry_depth"

    claim_series = "yes_ask" if claim_side == "yes" else "no_ask"
    fair_claim_prob = fair_prob_yes if claim_side == "yes" else (1.0 - fair_prob_yes)
    current_cex_mid = cex_book.mid() or 0.0
    claim_price_move_bps = 0.0
    fair_claim_move_bps = 0.0
    cex_mid_move_bps = 0.0
    pulse_score_bps = 0.0
    pocket_reference_price = entry["avg_price"]
    window_displacement_bps = 0.0
    refill_ratio = 0.0
    extreme_age_ms = 0

    if signal_mode == "flow-first":
        claim_window = market_tape.window_decimal_snapshot(claim_series, claim_book.ts_ms, params.pulse_window_ms)
        fair_window = market_tape.window_float_snapshot("fair_prob_yes", now_ms, params.pulse_window_ms)
        cex_window = asset_cex_tape.window_decimal_snapshot("cex_mid", cex_book.ts_ms, params.pulse_window_ms)
        current_claim_ask = claim_book.best_ask() or entry["avg_price"]
        if claim_window is None or fair_window is None or cex_window is None:
            return None, "missing_pulse_history"
        flow_metrics = compute_flow_reversion_metrics(
            window=claim_window,
            current_value=current_claim_ask,
            now_ms=claim_book.ts_ms,
        )
        if flow_metrics is None:
            return None, "no_flow_reversion_setup"
        pocket_reference_price = claim_window.max_value
        window_displacement_bps = flow_metrics.window_displacement_bps
        refill_ratio = flow_metrics.refill_ratio
        extreme_age_ms = flow_metrics.extreme_age_ms
        if pocket_reference_price > entry["avg_price"] > 0.0:
            claim_price_move_bps = ((pocket_reference_price - entry["avg_price"]) / pocket_reference_price) * 10_000.0
        fair_claim_move_bps = range_move_bps(fair_window)
        cex_mid_move_bps = range_move_bps(cex_window)
        pulse_score_bps = max(window_displacement_bps - fair_claim_move_bps, 0.0)
    else:
        prior_claim_ask = market_tape.latest_decimal_before(claim_series, claim_book.ts_ms, params.pulse_window_ms)
        prior_cex_mid = asset_cex_tape.latest_decimal_before("cex_mid", cex_book.ts_ms, params.pulse_window_ms)
        prior_fair_prob_yes = market_tape.latest_float_before("fair_prob_yes", now_ms, params.pulse_window_ms)
        if prior_claim_ask is None or prior_cex_mid is None or prior_fair_prob_yes is None:
            return None, "missing_pulse_history"
        pocket_reference_price = prior_claim_ask
        window_displacement_bps = 0.0
        if prior_claim_ask > entry["avg_price"] > 0.0:
            claim_price_move_bps = ((prior_claim_ask - entry["avg_price"]) / prior_claim_ask) * 10_000.0
        prior_fair_claim_prob = prior_fair_prob_yes if claim_side == "yes" else (1.0 - prior_fair_prob_yes)
        fair_claim_move_bps = abs(fair_claim_prob - prior_fair_claim_prob) * 10_000.0
        cex_mid_move_bps = abs(current_cex_mid - prior_cex_mid) / prior_cex_mid * 10_000.0 if prior_cex_mid > 0.0 else 0.0
        pulse_score_bps = max(claim_price_move_bps - fair_claim_move_bps, 0.0)
        window_displacement_bps = claim_price_move_bps

    if claim_price_move_bps < params.min_claim_price_move_bps:
        return None, "claim_move_too_small"
    if fair_claim_move_bps > params.max_fair_claim_move_bps:
        return None, "fair_move_too_large"
    if cex_mid_move_bps > params.max_cex_mid_move_bps:
        return None, "cex_move_too_large"
    if pulse_score_bps < params.min_pulse_score_bps:
        return None, "pulse_score_too_small"

    pocket_ticks = (
        max(math.floor((pocket_reference_price - entry["avg_price"]) / market.min_tick_size), 0.0)
        if pocket_reference_price > entry["avg_price"]
        else 0.0
    )
    target_exit_price = recommended_exit_price(
        entry["avg_price"],
        pulse_score_bps,
        pocket_ticks,
        market.min_tick_size,
        params,
    )
    fair_claim_price = fair_claim_prob
    target_exit_price = min(
        target_exit_price,
        max(pocket_reference_price, entry["avg_price"]),
        max(fair_claim_price, entry["avg_price"]),
        1.0,
    )
    timeout_exit_price = claim_book.executable_sell_shares(entry["filled_shares"]) or 0.0

    hedge_cost_bps = params.cex_slippage_bps
    fee_bps = params.poly_fee_bps + params.cex_taker_fee_bps
    reserve_bps = params.cex_taker_fee_bps + params.poly_fee_bps / 2.0
    expected_net_edge_bps = price_edge_bps(entry["avg_price"], target_exit_price) - hedge_cost_bps - fee_bps - reserve_bps
    expected_net_pnl_usd = entry["filled_notional_usd"] * expected_net_edge_bps / 10_000.0
    timeout_net_edge_bps = price_edge_bps(entry["avg_price"], timeout_exit_price) - hedge_cost_bps - fee_bps - reserve_bps
    timeout_net_pnl_usd = entry["filled_notional_usd"] * timeout_net_edge_bps / 10_000.0
    timeout_loss_estimate_usd = max(-timeout_net_pnl_usd, 0.0)
    if expected_net_edge_bps < params.min_net_session_edge_bps:
        return None, "net_edge_too_small"
    if expected_net_pnl_usd <= 0.0 or expected_net_pnl_usd < params.min_expected_net_pnl_usd:
        return None, "expected_pnl_too_small"

    current_mid = claim_book.mid() or entry["avg_price"]
    target_distance_to_mid_bps = abs(target_exit_price - current_mid) / current_mid * 10_000.0 if current_mid > 0.0 else 0.0
    predicted_hit_rate = estimate_base_hit_rate(target_distance_to_mid_bps, entry["filled_notional_usd"], entry["levels_consumed"], claim_price_move_bps, entry["depth_fill_ratio"])
    required_hit_rate_value = compute_required_hit_rate(
        expected_net_pnl_usd,
        timeout_loss_estimate_usd,
        params.min_expected_net_pnl_usd,
    )
    realizable_ev_usd = predicted_hit_rate * expected_net_pnl_usd + (1.0 - predicted_hit_rate) * timeout_net_pnl_usd
    if timeout_loss_estimate_usd > params.max_timeout_loss_usd:
        return None, "timeout_loss_too_large"
    if required_hit_rate_value > params.max_required_hit_rate:
        return None, "required_hit_rate_too_high"
    if predicted_hit_rate < required_hit_rate_value + 0.10:
        return None, "predicted_hit_rate_too_low"
    if realizable_ev_usd < 1.0:
        return None, "realizable_ev_too_low"

    instant_basis_bps = (fair_claim_price - entry["avg_price"]) * 10_000.0
    return CandidateRecord(
        ts_ms=now_ms,
        asset=market.asset,
        symbol=market.symbol,
        signal_mode=signal_mode,
        claim_side=claim_side,
        entry_price=entry["avg_price"],
        fair_prob_yes=fair_prob_yes,
        fair_claim_price=fair_claim_price,
        instant_basis_bps=instant_basis_bps,
        claim_price_move_bps=claim_price_move_bps,
        window_displacement_bps=window_displacement_bps,
        refill_ratio=refill_ratio,
        extreme_age_ms=extreme_age_ms,
        fair_claim_move_bps=fair_claim_move_bps,
        cex_mid_move_bps=cex_mid_move_bps,
        pulse_score_bps=pulse_score_bps,
        pocket_ticks=pocket_ticks,
        target_exit_price=target_exit_price,
        timeout_exit_price=timeout_exit_price,
        expected_net_edge_bps=expected_net_edge_bps,
        expected_net_pnl_usd=expected_net_pnl_usd,
        timeout_net_pnl_usd=timeout_net_pnl_usd,
        timeout_loss_estimate_usd=timeout_loss_estimate_usd,
        target_distance_to_mid_bps=target_distance_to_mid_bps,
        required_hit_rate=required_hit_rate_value,
        predicted_hit_rate=predicted_hit_rate,
        realizable_ev_usd=realizable_ev_usd,
        levels_consumed=entry["levels_consumed"],
        anchor_age_ms=anchor_age_ms,
        anchor_latency_delta_ms=anchor_latency_delta_ms,
    ), None


def recommended_exit_price(entry_price: float, pulse_score_bps: float, available_reversion_ticks: float, tick_size: float, params: RuntimeParams) -> float:
    if pulse_score_bps >= params.strong_pulse_score_bps:
        target_ticks = params.strong_target_ticks
    elif pulse_score_bps >= params.medium_pulse_score_bps:
        target_ticks = params.medium_target_ticks
    else:
        target_ticks = params.base_target_ticks
    if math.isfinite(available_reversion_ticks) and available_reversion_ticks > 0.0:
        cap_ticks = math.floor(available_reversion_ticks)
        if cap_ticks > 0:
            target_ticks = min(target_ticks, max(int(cap_ticks), params.base_target_ticks))
    return min(entry_price + tick_size * target_ticks, 1.0)


def price_edge_bps(entry_price: float, exit_price: float) -> float:
    if entry_price <= 0.0:
        return 0.0
    return ((exit_price - entry_price) / entry_price) * 10_000.0


def estimate_base_hit_rate(
    target_distance_to_mid_bps: float,
    swept_notional_usd: float,
    swept_levels_count: int,
    post_pulse_depth_gap_bps: float,
    depth_fill_ratio: float,
) -> float:
    base = 0.0
    if target_distance_to_mid_bps <= 100.0:
        base = 0.40
    elif target_distance_to_mid_bps <= 200.0:
        base = 0.35
    elif target_distance_to_mid_bps <= 300.0:
        base = 0.10
    elif target_distance_to_mid_bps <= 500.0:
        base = 0.05
    if swept_notional_usd >= 400.0 and swept_levels_count >= 3:
        base += 0.15
    elif swept_notional_usd >= 300.0 and swept_levels_count >= 2:
        base += 0.10
    if post_pulse_depth_gap_bps >= 120.0:
        base += 0.05
    if depth_fill_ratio < 0.8:
        base -= 0.05
    return max(0.0, min(base, 0.90))


def compute_required_hit_rate(expected_net_pnl_usd: float, timeout_loss_estimate_usd: float, min_expected_net_pnl_usd: float) -> float:
    if timeout_loss_estimate_usd <= 0.0:
        return 0.0
    if expected_net_pnl_usd <= 0.0:
        return float("inf")
    denominator = expected_net_pnl_usd + timeout_loss_estimate_usd
    if denominator <= 0.0:
        return float("inf")
    return (timeout_loss_estimate_usd + max(min_expected_net_pnl_usd, 0.0)) / denominator


def compute_horizon_price_outcomes(
    *,
    times_ms: list[int],
    best_bids: list[float],
    last_trades: list[float],
    horizon_ms: int,
) -> HorizonPriceOutcome:
    if not (len(times_ms) == len(best_bids) == len(last_trades)):
        raise ValueError("times, bids, trades length mismatch")
    n = len(times_ms)
    if n == 0:
        return HorizonPriceOutcome(future_max_bid=[], future_max_touch=[], deadline_bid=[])

    future_max_bid = [0.0] * n
    future_max_touch = [0.0] * n
    deadline_bid = [0.0] * n
    bid_deque: deque[int] = deque()
    touch_deque: deque[int] = deque()
    touches = [max(best_bid, last_trade) for best_bid, last_trade in zip(best_bids, last_trades, strict=True)]
    end = -1

    for start in range(n):
        deadline_ts = times_ms[start] + horizon_ms
        while end + 1 < n and times_ms[end + 1] <= deadline_ts:
            end += 1
            while bid_deque and best_bids[bid_deque[-1]] <= best_bids[end]:
                bid_deque.pop()
            bid_deque.append(end)
            while touch_deque and touches[touch_deque[-1]] <= touches[end]:
                touch_deque.pop()
            touch_deque.append(end)
        while bid_deque and bid_deque[0] < start:
            bid_deque.popleft()
        while touch_deque and touch_deque[0] < start:
            touch_deque.popleft()
        future_max_bid[start] = best_bids[bid_deque[0]] if bid_deque else 0.0
        future_max_touch[start] = touches[touch_deque[0]] if touch_deque else 0.0
        deadline_bid[start] = best_bids[end] if end >= start else best_bids[start]

    return HorizonPriceOutcome(
        future_max_bid=future_max_bid,
        future_max_touch=future_max_touch,
        deadline_bid=deadline_bid,
    )


def derive_aggressive_exit_frame(
    *,
    frame: pd.DataFrame,
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
) -> pd.DataFrame:
    derived = frame.copy()
    raw_target = derived["entry_price"] + derived["min_tick_size"] * exit_config.target_ticks
    if exit_config.target_mode == "cap_window":
        derived["target_exit_price"] = raw_target.clip(upper=derived["window_high_price"].clip(lower=derived["entry_price"]))
    elif exit_config.target_mode == "cap_window_fair":
        cap = pd.concat(
            [
                derived["window_high_price"].clip(lower=derived["entry_price"]),
                derived["fair_claim_price"].clip(lower=derived["entry_price"]),
            ],
            axis=1,
        ).min(axis=1)
        derived["target_exit_price"] = raw_target.clip(upper=cap)
    else:
        derived["target_exit_price"] = raw_target

    if exit_config.touch_mode == "bid_touch":
        touch_column = f"outcome_future_max_bid_{exit_config.horizon_label}"
    else:
        touch_column = f"outcome_future_max_touch_{exit_config.horizon_label}"
    deadline_column = f"outcome_deadline_bid_{exit_config.horizon_label}"

    derived["touch_price"] = derived[touch_column]
    derived["deadline_exit_price"] = derived[deadline_column]
    derived["hit_target"] = derived["touch_price"] >= derived["target_exit_price"]
    derived["realized_exit_price"] = derived["deadline_exit_price"].where(
        ~derived["hit_target"],
        derived["target_exit_price"],
    )

    fee_bps = poly_fee_bps + cex_taker_fee_bps
    reserve_bps = cex_taker_fee_bps + poly_fee_bps / 2.0
    net_edge_bps = (
        (derived["realized_exit_price"] - derived["entry_price"]) / derived["entry_price"] * 10_000.0
        - cex_slippage_bps
        - fee_bps
        - reserve_bps
    )
    derived["realized_net_pnl_usd"] = derived["filled_notional_usd"] * net_edge_bps / 10_000.0
    derived["realized_hit"] = derived["hit_target"].astype(int)
    derived["exit_config_name"] = (
        f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}"
    )
    return derived


def rank_aggressive_signal_rules(
    *,
    frame: pd.DataFrame,
    min_trades: int,
    top_k: int,
    cooldown_ms: int = 0,
) -> list[dict[str, Any]]:
    candidate_masks = build_aggressive_rule_masks(frame)
    ranked: list[dict[str, Any]] = []

    def _maybe_append(rule_name: str, mask: pd.Series) -> None:
        subset = frame.loc[mask]
        if cooldown_ms > 0 and {"ts_ms", "symbol", "claim_side"}.issubset(subset.columns):
            subset = apply_rule_cooldown(subset, cooldown_ms)
        trade_count = int(len(subset))
        if trade_count < min_trades:
            return
        total_realized = float(subset["realized_net_pnl_usd"].sum())
        avg_realized = float(subset["realized_net_pnl_usd"].mean())
        if avg_realized <= 0.0:
            return
        ranked.append(
            {
                "rule_name": rule_name,
                "trade_count": trade_count,
                "total_realized_pnl_usd": total_realized,
                "avg_realized_pnl_usd": avg_realized,
                "hit_rate": float(subset["realized_hit"].mean()),
                "exit_config_name": str(subset["exit_config_name"].iloc[0]),
            }
        )

    for rule_name, mask in candidate_masks:
        _maybe_append(rule_name, mask)

    for left_index, (left_name, left_mask) in enumerate(candidate_masks):
        for right_name, right_mask in candidate_masks[left_index + 1 :]:
            combined = left_mask & right_mask
            _maybe_append(f"{left_name} & {right_name}", combined)

    ranked.sort(key=lambda item: (item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    return ranked[:top_k]


def apply_rule_cooldown(frame: pd.DataFrame, cooldown_ms: int) -> pd.DataFrame:
    if frame.empty or cooldown_ms <= 0:
        return frame
    ordered = frame.sort_values(["symbol", "claim_side", "ts_ms"])
    kept_indices: list[Any] = []
    last_ts_by_key: dict[tuple[str, str], int] = {}
    for row in ordered.itertuples():
        key = (str(row.symbol), str(row.claim_side))
        ts_ms = int(row.ts_ms)
        last_ts = last_ts_by_key.get(key)
        if last_ts is not None and ts_ms - last_ts < cooldown_ms:
            continue
        kept_indices.append(row.Index)
        last_ts_by_key[key] = ts_ms
    return ordered.loc[kept_indices]


def aligned_tick_floor_bps(entry_price: float, tick_size: float, min_distance_bps: float) -> float:
    if not math.isfinite(entry_price) or entry_price <= 0.0:
        return float("inf")
    if not math.isfinite(tick_size) or tick_size <= 0.0:
        return float("inf")
    if not math.isfinite(min_distance_bps):
        return float("inf")

    min_target_price = entry_price * (1.0 + max(min_distance_bps, 0.0) / 10_000.0)
    raw_ticks = math.ceil(max(min_target_price - entry_price, 0.0) / tick_size - 1e-12)
    target_price = entry_price + max(raw_ticks, 0) * tick_size
    return price_edge_bps(entry_price, target_price)


def derive_dislocation_reachability_frame(
    *,
    frame: pd.DataFrame,
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_expected_net_pnl_usd: float,
) -> pd.DataFrame:
    derived = derive_aggressive_exit_frame(
        frame=frame,
        poly_fee_bps=poly_fee_bps,
        cex_taker_fee_bps=cex_taker_fee_bps,
        cex_slippage_bps=cex_slippage_bps,
        exit_config=exit_config,
    )

    fee_bps = poly_fee_bps + cex_taker_fee_bps
    reserve_bps = cex_taker_fee_bps + poly_fee_bps / 2.0
    gross_cost_bps = cex_slippage_bps + fee_bps + reserve_bps

    target_exit = pd.to_numeric(_frame_column(derived, "target_exit_price", 0.0), errors="coerce").fillna(0.0)
    current_mid = pd.to_numeric(_frame_column(derived, "current_mid", 0.0), errors="coerce").fillna(0.0)
    timeout_exit = pd.to_numeric(_frame_column(derived, "timeout_exit_price", 0.0), errors="coerce").fillna(0.0)
    entry_price = pd.to_numeric(_frame_column(derived, "entry_price", 0.0), errors="coerce").fillna(0.0)
    tick_size = pd.to_numeric(_frame_column(derived, "min_tick_size", 0.0), errors="coerce").fillna(0.0)
    filled_notional = pd.to_numeric(_frame_column(derived, "filled_notional_usd", 0.0), errors="coerce").fillna(0.0)
    levels_consumed = pd.to_numeric(_frame_column(derived, "levels_consumed", 0), errors="coerce").fillna(0).astype(int)
    depth_fill_ratio = pd.to_numeric(_frame_column(derived, "depth_fill_ratio", 1.0), errors="coerce").fillna(1.0)

    expected_net_edge_bps = (
        (target_exit - entry_price) / entry_price.where(entry_price > 0.0, 1.0) * 10_000.0
        - gross_cost_bps
    )
    timeout_net_edge_bps = (
        (timeout_exit - entry_price) / entry_price.where(entry_price > 0.0, 1.0) * 10_000.0
        - gross_cost_bps
    )
    expected_net_pnl = filled_notional * expected_net_edge_bps / 10_000.0
    timeout_net_pnl = filled_notional * timeout_net_edge_bps / 10_000.0
    timeout_loss_estimate = (-timeout_net_pnl).clip(lower=0.0)
    fixed_target_distance = (
        (target_exit - current_mid).abs() / current_mid.where(current_mid > 0.0, 1.0) * 10_000.0
    )
    required_hit_rate = [
        compute_required_hit_rate(
            expected_net_pnl_usd=float(expected),
            timeout_loss_estimate_usd=float(timeout_loss),
            min_expected_net_pnl_usd=min_expected_net_pnl_usd,
        )
        for expected, timeout_loss in zip(expected_net_pnl, timeout_loss_estimate, strict=False)
    ]
    predicted_hit_rate = [
        estimate_base_hit_rate(
            target_distance_to_mid_bps=float(distance),
            swept_notional_usd=float(notional),
            swept_levels_count=int(levels),
            post_pulse_depth_gap_bps=0.0,
            depth_fill_ratio=float(fill_ratio),
        )
        for distance, notional, levels, fill_ratio in zip(
            fixed_target_distance,
            filled_notional,
            levels_consumed,
            depth_fill_ratio,
            strict=False,
        )
    ]
    realizable_ev = [
        predicted * expected + (1.0 - predicted) * timeout
        for predicted, expected, timeout in zip(
            predicted_hit_rate,
            expected_net_pnl,
            timeout_net_pnl,
            strict=False,
        )
    ]

    derived["fixed_target_exit_price"] = target_exit
    derived["min_profitable_target_distance_bps"] = gross_cost_bps
    derived["min_realizable_target_distance_bps"] = [
        aligned_tick_floor_bps(
            entry_price=float(entry),
            tick_size=float(tick),
            min_distance_bps=gross_cost_bps,
        )
        for entry, tick in zip(entry_price, tick_size, strict=False)
    ]
    derived["fixed_target_distance_to_mid_bps"] = fixed_target_distance
    derived["fixed_target_expected_net_pnl_usd"] = expected_net_pnl
    derived["fixed_target_timeout_net_pnl_usd"] = timeout_net_pnl
    derived["fixed_target_timeout_loss_estimate_usd"] = timeout_loss_estimate
    derived["fixed_target_required_hit_rate"] = required_hit_rate
    derived["fixed_target_predicted_hit_rate"] = predicted_hit_rate
    derived["fixed_target_realizable_ev_usd"] = realizable_ev
    valid_tick_size = tick_size.where(tick_size > 0.0, math.nan)
    derived["boundary_headroom_ticks"] = pd.concat(
        [
            (1.0 - entry_price) / valid_tick_size,
            entry_price / valid_tick_size,
        ],
        axis=1,
    ).min(axis=1).round(6)
    return derived


def build_dislocation_reachability_strategy_masks(frame: pd.DataFrame) -> list[tuple[str, list[str], pd.Series]]:
    if frame.empty:
        return []

    mask_map = dict(build_aggressive_rule_masks(frame))
    base_mask = (
        mask_map["setup=directional_dislocation"]
        & mask_map["disp>=1500"]
        & mask_map["refill<=0.35"]
    )
    min_realizable = pd.to_numeric(
        _frame_column(frame, "min_realizable_target_distance_bps", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    fixed_target_distance = pd.to_numeric(
        _frame_column(frame, "fixed_target_distance_to_mid_bps", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    required_hit_rate = pd.to_numeric(
        _frame_column(frame, "fixed_target_required_hit_rate", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    predicted_hit_rate = pd.to_numeric(
        _frame_column(frame, "fixed_target_predicted_hit_rate", 0.0),
        errors="coerce",
    ).fillna(0.0)

    return [
        ("structure_core", ["setup=directional_dislocation", "disp>=1500", "refill<=0.35"], base_mask),
        (
            "reachable_core_150",
            [
                "setup=directional_dislocation",
                "disp>=1500",
                "refill<=0.35",
                "min_realizable_target_distance_bps<=150",
                "fixed_target_required_hit_rate<=0.60",
            ],
            base_mask & (min_realizable <= 150.0) & (required_hit_rate <= 0.60),
        ),
        (
            "reachable_core_200",
            [
                "setup=directional_dislocation",
                "disp>=1500",
                "refill<=0.35",
                "min_realizable_target_distance_bps<=200",
                "fixed_target_required_hit_rate<=0.60",
            ],
            base_mask & (min_realizable <= 200.0) & (required_hit_rate <= 0.60),
        ),
        (
            "reachable_snapback_120",
            [
                "setup=directional_dislocation",
                "disp>=1500",
                "refill<=0.35",
                "min_realizable_target_distance_bps<=150",
                "fixed_target_distance_to_mid_bps<=120",
                "fixed_target_required_hit_rate<=0.60",
                "fixed_target_predicted_hit_rate>=required+10pp",
            ],
            base_mask
            & (min_realizable <= 150.0)
            & (fixed_target_distance <= 120.0)
            & (required_hit_rate <= 0.60)
            & (predicted_hit_rate >= (required_hit_rate + 0.10)),
        ),
    ]


def _median_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = float(series.median())
    return value if math.isfinite(value) else None


def evaluate_dislocation_reachability_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_expected_net_pnl_usd: float,
    min_trades_per_day: int = 1,
) -> dict[str, Any]:
    if not frames_by_label:
        return {"strategy_stats": [], "positive_strategy_count": 0}

    derived_by_label: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_label.items():
        if frame.empty:
            return {"strategy_stats": [], "positive_strategy_count": 0}
        derived_by_label[label] = derive_dislocation_reachability_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
            min_expected_net_pnl_usd=min_expected_net_pnl_usd,
        )

    ordered_labels = list(frames_by_label.keys())
    cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
    strategy_stats: list[dict[str, Any]] = []
    positive_strategy_count = 0
    for strategy_name, filters, _ in build_dislocation_reachability_strategy_masks(derived_by_label[ordered_labels[0]]):
        total_pnl = 0.0
        total_trades = 0
        passes_cross_day = True
        day_stats: dict[str, dict[str, float | int | None]] = {}
        for label in ordered_labels:
            derived = derived_by_label[label]
            strategy_mask_map = {
                name: mask for name, _, mask in build_dislocation_reachability_strategy_masks(derived)
            }
            subset = apply_rule_cooldown(derived.loc[strategy_mask_map[strategy_name]], cooldown_ms)
            trade_count = int(len(subset))
            pnl = float(subset["realized_net_pnl_usd"].sum()) if trade_count > 0 else 0.0
            hit_rate = float(subset["realized_hit"].mean()) if trade_count > 0 else 0.0
            day_stats[label] = {
                "trade_count": trade_count,
                "total_realized_pnl_usd": pnl,
                "hit_rate": hit_rate,
                "min_realizable_target_distance_bps_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "min_realizable_target_distance_bps", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "fixed_target_distance_to_mid_bps_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "fixed_target_distance_to_mid_bps", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "fixed_target_required_hit_rate_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "fixed_target_required_hit_rate", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
            }
            if trade_count < min_trades_per_day or pnl <= 0.0:
                passes_cross_day = False
            total_pnl += pnl
            total_trades += trade_count
        if passes_cross_day:
            positive_strategy_count += 1
        strategy_stats.append(
            {
                "strategy_name": strategy_name,
                "filters": filters,
                "passes_cross_day": passes_cross_day,
                "total_realized_pnl_usd": total_pnl,
                "total_trade_count": total_trades,
                "avg_realized_pnl_usd": total_pnl / total_trades if total_trades > 0 else 0.0,
                "min_day_pnl_usd": min(float(stats["total_realized_pnl_usd"]) for stats in day_stats.values()),
                "day_stats": day_stats,
                "exit_config_name": (
                    f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:"
                    f"ticks{exit_config.target_ticks}"
                ),
            }
        )
    return {
        "strategy_stats": strategy_stats,
        "positive_strategy_count": positive_strategy_count,
    }


def run_dislocation_reachability_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    output_dir: Path,
    label_prefix: str = "dislocation_reachability_15m",
    poly_fee_bps: float = 20.0,
    cex_taker_fee_bps: float = 5.0,
    cex_slippage_bps: float = 2.0,
    exit_config: AggressiveExitConfig | None = None,
    min_expected_net_pnl_usd: float = 1.0,
    min_trades_per_day: int = 1,
) -> Path:
    fixed_exit_config = exit_config or AggressiveExitConfig("15m", 1, "any_touch", "raw_tick")
    evaluated = evaluate_dislocation_reachability_matrix(
        frames_by_label=frames_by_label,
        poly_fee_bps=poly_fee_bps,
        cex_taker_fee_bps=cex_taker_fee_bps,
        cex_slippage_bps=cex_slippage_bps,
        exit_config=fixed_exit_config,
        min_expected_net_pnl_usd=min_expected_net_pnl_usd,
        min_trades_per_day=min_trades_per_day,
    )
    payload = {
        "labels": list(frames_by_label.keys()),
        "min_trades_per_day": min_trades_per_day,
        "exit_config_name": (
            f"{fixed_exit_config.horizon_label}:{fixed_exit_config.touch_mode}:"
            f"{fixed_exit_config.target_mode}:ticks{fixed_exit_config.target_ticks}"
        ),
        **evaluated,
    }
    return write_checkpoint(output_dir, "dislocation_reachability_matrix", label_prefix, payload)


def build_geometry_signal_strategy_masks(
    frame: pd.DataFrame,
    *,
    symbol_filters: set[str] | None = None,
) -> list[tuple[str, list[str], pd.Series]]:
    if frame.empty:
        return []

    mask_map = dict(build_aggressive_rule_masks(frame))
    fixed_target_distance = pd.to_numeric(
        _frame_column(frame, "fixed_target_distance_to_mid_bps", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    min_realizable = pd.to_numeric(
        _frame_column(frame, "min_realizable_target_distance_bps", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    window_displacement = pd.to_numeric(
        _frame_column(frame, "window_displacement_bps", 0.0),
        errors="coerce",
    ).fillna(0.0)
    anchor_follow = pd.to_numeric(
        _frame_column(frame, "anchor_follow_ratio", float("inf")),
        errors="coerce",
    ).fillna(float("inf"))
    levels_consumed = pd.to_numeric(
        _frame_column(frame, "levels_consumed", 0),
        errors="coerce",
    ).fillna(0.0)
    boundary_headroom_ticks = pd.to_numeric(
        _frame_column(frame, "boundary_headroom_ticks", float("nan")),
        errors="coerce",
    )
    symbol_mask = pd.Series([True] * len(frame), index=frame.index)
    if symbol_filters:
        symbol_mask = frame["symbol"].isin(symbol_filters)

    universe = (
        symbol_mask
        & mask_map["rule=directional"]
        & mask_map["tts>=24h"]
        & (fixed_target_distance >= 100.0)
        & (fixed_target_distance <= 200.0)
        & (min_realizable <= 200.0)
    )
    tradable_geom = universe & (boundary_headroom_ticks > 10.0)
    strong_geom = universe & (boundary_headroom_ticks > 20.0)
    shock_core = tradable_geom & (window_displacement >= 40.0)
    strong_shock_core = strong_geom & (window_displacement >= 40.0)
    return [
        (
            "geom_headroom10_core",
            [
                "rule=directional",
                "tts>=24h",
                "fixed_target_distance_to_mid_bps in [100,200]",
                "min_realizable_target_distance_bps<=200",
                "boundary_headroom_ticks>10",
                "disp>=40",
            ],
            shock_core,
        ),
        (
            "geom_headroom20_core",
            [
                "rule=directional",
                "tts>=24h",
                "fixed_target_distance_to_mid_bps in [100,200]",
                "min_realizable_target_distance_bps<=200",
                "boundary_headroom_ticks>20",
                "disp>=40",
            ],
            strong_shock_core,
        ),
        (
            "geom_elastic_snapback",
            [
                "geom_headroom20_core",
                "levels_consumed>=2",
                "anchor_follow_ratio>0.60",
            ],
            strong_shock_core & (levels_consumed >= 2.0) & (anchor_follow > 0.60),
        ),
        (
            "geom_deep_reversion",
            [
                "geom_headroom20_core",
                "levels_consumed>=2",
                "anchor_follow_ratio<=0.15",
            ],
            strong_shock_core & (levels_consumed >= 2.0) & (anchor_follow <= 0.15),
        ),
        (
            "geom_terminal_zone_control",
            [
                "rule=directional",
                "tts>=24h",
                "fixed_target_distance_to_mid_bps in [100,200]",
                "min_realizable_target_distance_bps<=200",
                "boundary_headroom_ticks<=5",
            ],
            universe & (boundary_headroom_ticks <= 5.0),
        ),
    ]


def evaluate_geometry_signal_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_expected_net_pnl_usd: float,
    min_trades_per_day: int = 1,
    symbol_filters: set[str] | None = None,
) -> dict[str, Any]:
    if not frames_by_label:
        return {"strategy_stats": [], "positive_strategy_count": 0, "symbol_filters": []}

    derived_by_label: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_label.items():
        if frame.empty:
            return {"strategy_stats": [], "positive_strategy_count": 0, "symbol_filters": []}
        derived_by_label[label] = derive_dislocation_reachability_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
            min_expected_net_pnl_usd=min_expected_net_pnl_usd,
        )

    ordered_labels = list(frames_by_label.keys())
    cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
    strategy_stats: list[dict[str, Any]] = []
    positive_strategy_count = 0
    for strategy_name, filters, _ in build_geometry_signal_strategy_masks(
        derived_by_label[ordered_labels[0]],
        symbol_filters=symbol_filters,
    ):
        total_pnl = 0.0
        total_trades = 0
        passes_cross_day = True
        day_stats: dict[str, dict[str, float | int | None]] = {}
        for label in ordered_labels:
            derived = derived_by_label[label]
            strategy_mask_map = {
                name: mask
                for name, _, mask in build_geometry_signal_strategy_masks(
                    derived,
                    symbol_filters=symbol_filters,
                )
            }
            subset = apply_rule_cooldown(derived.loc[strategy_mask_map[strategy_name]], cooldown_ms)
            trade_count = int(len(subset))
            pnl = float(subset["realized_net_pnl_usd"].sum()) if trade_count > 0 else 0.0
            hit_rate = float(subset["realized_hit"].mean()) if trade_count > 0 else 0.0
            day_stats[label] = {
                "trade_count": trade_count,
                "total_realized_pnl_usd": pnl,
                "hit_rate": hit_rate,
                "fixed_target_distance_to_mid_bps_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "fixed_target_distance_to_mid_bps", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "window_displacement_bps_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "window_displacement_bps", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "post_pulse_refill_ratio_3s_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "post_pulse_refill_ratio_3s", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "anchor_follow_ratio_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "anchor_follow_ratio", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
                "boundary_headroom_ticks_p50": _median_or_none(
                    pd.to_numeric(
                        _frame_column(subset, "boundary_headroom_ticks", float("nan")),
                        errors="coerce",
                    ).dropna()
                ),
            }
            if trade_count < min_trades_per_day or pnl <= 0.0:
                passes_cross_day = False
            total_pnl += pnl
            total_trades += trade_count
        if passes_cross_day:
            positive_strategy_count += 1
        strategy_stats.append(
            {
                "strategy_name": strategy_name,
                "filters": filters,
                "passes_cross_day": passes_cross_day,
                "total_realized_pnl_usd": total_pnl,
                "total_trade_count": total_trades,
                "avg_realized_pnl_usd": total_pnl / total_trades if total_trades > 0 else 0.0,
                "min_day_pnl_usd": min(float(stats["total_realized_pnl_usd"]) for stats in day_stats.values()),
                "day_stats": day_stats,
                "exit_config_name": (
                    f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:"
                    f"ticks{exit_config.target_ticks}"
                ),
            }
        )
    return {
        "strategy_stats": strategy_stats,
        "positive_strategy_count": positive_strategy_count,
        "symbol_filters": sorted(symbol_filters) if symbol_filters else [],
    }


def run_geometry_signal_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    output_dir: Path,
    label_prefix: str = "geometry_signal_15m",
    poly_fee_bps: float = 20.0,
    cex_taker_fee_bps: float = 5.0,
    cex_slippage_bps: float = 2.0,
    exit_config: AggressiveExitConfig | None = None,
    min_expected_net_pnl_usd: float = 0.5,
    min_trades_per_day: int = 1,
    symbol_filters: set[str] | None = None,
) -> Path:
    fixed_exit_config = exit_config or AggressiveExitConfig("15m", 1, "any_touch", "raw_tick")
    evaluated = evaluate_geometry_signal_matrix(
        frames_by_label=frames_by_label,
        poly_fee_bps=poly_fee_bps,
        cex_taker_fee_bps=cex_taker_fee_bps,
        cex_slippage_bps=cex_slippage_bps,
        exit_config=fixed_exit_config,
        min_expected_net_pnl_usd=min_expected_net_pnl_usd,
        min_trades_per_day=min_trades_per_day,
        symbol_filters=symbol_filters,
    )
    payload = {
        "labels": list(frames_by_label.keys()),
        "min_trades_per_day": min_trades_per_day,
        "exit_config_name": (
            f"{fixed_exit_config.horizon_label}:{fixed_exit_config.touch_mode}:"
            f"{fixed_exit_config.target_mode}:ticks{fixed_exit_config.target_ticks}"
        ),
        **evaluated,
    }
    return write_checkpoint(output_dir, "geometry_signal_matrix", label_prefix, payload)


def build_aggressive_rule_masks(frame: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    entry_price = _frame_column(frame, "entry_price", 0.0)
    fair_claim_price = _frame_column(frame, "fair_claim_price", 0.0)
    current_ask = _frame_column(frame, "current_ask", 0.0)
    current_bid = _frame_column(frame, "current_bid", 0.0)
    current_mid = _frame_column(frame, "current_mid", 1.0)
    window_high_price = _frame_column(frame, "window_high_price", 0.0)
    min_tick_size = _frame_column(frame, "min_tick_size", 1.0)
    window_displacement_bps = _frame_column(frame, "window_displacement_bps", 0.0)
    flow_residual_gap_bps = _frame_column(frame, "flow_residual_gap_bps", 0.0)
    flow_refill_ratio = _frame_column(frame, "flow_refill_ratio", 1.0)
    fair_claim_move_bps = _frame_column(frame, "fair_claim_move_bps", float("inf"))
    cex_mid_move_bps = _frame_column(frame, "cex_mid_move_bps", float("inf"))
    flow_extreme_age_ms = _frame_column(frame, "flow_extreme_age_ms", 1_000_000_000)
    market_rule_kind = _frame_column(frame, "market_rule_kind", "unknown")
    time_to_settlement_hours = _frame_column(frame, "time_to_settlement_hours", 0.0)
    anchor_follow = _frame_column(frame, "anchor_follow_ratio", float("inf"))

    basis_bps = _frame_column(frame, "residual_basis_bps", 0.0)
    if "residual_basis_bps" not in frame.columns:
        basis_bps = (fair_claim_price - entry_price) / entry_price.where(entry_price > 0.0, 1.0) * 10_000.0
    spread_bps = (current_ask - current_bid) / current_mid.where(current_mid > 0.0, 1.0) * 10_000.0
    pocket_ticks = (window_high_price - entry_price) / min_tick_size.where(min_tick_size > 0.0, 1.0)
    rule_directional = market_rule_kind.isin(["above", "below"])
    entry_le_015 = entry_price <= 0.15
    tts_gte_24h = time_to_settlement_hours >= 24.0
    anchor_follow_le_010 = anchor_follow <= 0.10
    anchor_follow_le_025 = anchor_follow <= 0.25
    directional_dislocation = rule_directional & entry_le_015 & tts_gte_24h & (basis_bps >= 300.0) & anchor_follow_le_025

    return [
        ("asset=btc", frame["asset"] == "btc"),
        ("asset=eth", frame["asset"] == "eth"),
        ("side=yes", frame["claim_side"] == "yes"),
        ("side=no", frame["claim_side"] == "no"),
        ("entry<=0.10", frame["entry_price"] <= 0.10),
        ("entry<=0.15", entry_le_015),
        ("entry<=0.25", frame["entry_price"] <= 0.25),
        ("entry<=0.50", frame["entry_price"] <= 0.50),
        ("entry>=0.70", frame["entry_price"] >= 0.70),
        ("rule=directional", rule_directional),
        ("rule=between", market_rule_kind == "between"),
        ("tts>=6h", time_to_settlement_hours >= 6.0),
        ("tts>=24h", tts_gte_24h),
        ("basis>=100bps", basis_bps >= 100.0),
        ("basis>=300bps", basis_bps >= 300.0),
        ("anchor_follow<=0.10", anchor_follow_le_010),
        ("anchor_follow<=0.25", anchor_follow_le_025),
        ("setup=directional_dislocation", directional_dislocation),
        ("refill<=0.10", flow_refill_ratio <= 0.10),
        ("refill<=0.20", flow_refill_ratio <= 0.20),
        ("refill<=0.35", flow_refill_ratio <= 0.35),
        ("disp>=500", window_displacement_bps >= 500.0),
        ("disp>=1000", window_displacement_bps >= 1000.0),
        ("disp>=1500", window_displacement_bps >= 1500.0),
        ("residual>=250", flow_residual_gap_bps >= 250.0),
        ("residual>=500", flow_residual_gap_bps >= 500.0),
        ("fair_move<=10", fair_claim_move_bps <= 10.0),
        ("fair_move<=25", fair_claim_move_bps <= 25.0),
        ("cex_move<=10", cex_mid_move_bps <= 10.0),
        ("cex_move<=25", cex_mid_move_bps <= 25.0),
        ("extreme_age<=1000", flow_extreme_age_ms <= 1000),
        ("extreme_age<=3000", flow_extreme_age_ms <= 3000),
        ("spread<=150bps", spread_bps <= 150.0),
        ("spread<=400bps", spread_bps <= 400.0),
        ("pocket>=2ticks", pocket_ticks >= 2.0),
        ("pocket>=5ticks", pocket_ticks >= 5.0),
    ]


def build_dislocation_strategy_masks(frame: pd.DataFrame) -> list[tuple[str, list[str], pd.Series]]:
    mask_map = dict(build_aggressive_rule_masks(frame))
    confirm_window_complete_1s = _frame_column(frame, "confirm_window_complete_1s", False).astype(bool)
    confirm_window_complete_3s = _frame_column(frame, "confirm_window_complete_3s", False).astype(bool)
    refill_1s = pd.to_numeric(_frame_column(frame, "post_pulse_refill_ratio_1s", float("nan")), errors="coerce")
    refill_3s = pd.to_numeric(_frame_column(frame, "post_pulse_refill_ratio_3s", float("nan")), errors="coerce")
    spread_snapback = pd.to_numeric(_frame_column(frame, "spread_snapback_bps", float("nan")), errors="coerce")
    depth_rebuild = pd.to_numeric(_frame_column(frame, "bid_depth_rebuild_ratio", float("nan")), errors="coerce")

    base_mask = (
        mask_map["setup=directional_dislocation"]
        & mask_map["disp>=1500"]
        & mask_map["refill<=0.35"]
    )
    extra_masks: dict[str, pd.Series] = {
        "refill1s_fast": confirm_window_complete_1s & (refill_1s >= 0.20),
        "refill3s_fast": confirm_window_complete_3s & (refill_3s >= 0.35),
        "spread_snapback": confirm_window_complete_3s & (spread_snapback >= 50.0),
        "depth_rebuild": confirm_window_complete_3s & (depth_rebuild >= 1.25),
    }
    strategy_specs = [
        ("structure_core", ["setup=directional_dislocation", "disp>=1500", "refill<=0.35"], []),
        (
            "structure_core + refill1s_fast",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "refill1s_fast"],
            ["refill1s_fast"],
        ),
        (
            "structure_core + refill3s_fast",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "refill3s_fast"],
            ["refill3s_fast"],
        ),
        (
            "structure_core + spread_snapback",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "spread_snapback"],
            ["spread_snapback"],
        ),
        (
            "structure_core + depth_rebuild",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "depth_rebuild"],
            ["depth_rebuild"],
        ),
        (
            "structure_core + refill3s_fast + spread_snapback",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "refill3s_fast", "spread_snapback"],
            ["refill3s_fast", "spread_snapback"],
        ),
        (
            "structure_core + refill3s_fast + depth_rebuild",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "refill3s_fast", "depth_rebuild"],
            ["refill3s_fast", "depth_rebuild"],
        ),
        (
            "structure_core + spread_snapback + depth_rebuild",
            ["setup=directional_dislocation", "disp>=1500", "refill<=0.35", "spread_snapback", "depth_rebuild"],
            ["spread_snapback", "depth_rebuild"],
        ),
        (
            "structure_core + refill3s_fast + spread_snapback + depth_rebuild",
            [
                "setup=directional_dislocation",
                "disp>=1500",
                "refill<=0.35",
                "refill3s_fast",
                "spread_snapback",
                "depth_rebuild",
            ],
            ["refill3s_fast", "spread_snapback", "depth_rebuild"],
        ),
    ]
    built: list[tuple[str, list[str], pd.Series]] = []
    for strategy_name, filters, extra_names in strategy_specs:
        mask = base_mask.copy()
        for extra_name in extra_names:
            mask = mask & extra_masks[extra_name]
        built.append((strategy_name, filters, mask))
    return built


def rank_dislocation_strategy_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_trades_per_day: int = 1,
) -> list[dict[str, Any]]:
    if not frames_by_label:
        return []

    derived_by_label: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_label.items():
        if frame.empty:
            return []
        derived_by_label[label] = derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
        )

    ordered_labels = list(frames_by_label.keys())
    cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
    ranked: list[dict[str, Any]] = []
    baseline_total_pnl: float | None = None
    for strategy_name, filters, _ in build_dislocation_strategy_masks(derived_by_label[ordered_labels[0]]):
        total_pnl = 0.0
        total_trades = 0
        keep = True
        day_stats: dict[str, dict[str, float | int]] = {}
        for label in ordered_labels:
            derived = derived_by_label[label]
            strategy_mask_map = {name: mask for name, _, mask in build_dislocation_strategy_masks(derived)}
            subset = apply_rule_cooldown(derived.loc[strategy_mask_map[strategy_name]], cooldown_ms)
            trade_count = int(len(subset))
            pnl = float(subset["realized_net_pnl_usd"].sum()) if trade_count > 0 else 0.0
            hit_rate = float(subset["realized_hit"].mean()) if trade_count > 0 else 0.0
            day_stats[label] = {
                "trade_count": trade_count,
                "total_realized_pnl_usd": pnl,
                "hit_rate": hit_rate,
            }
            if trade_count < min_trades_per_day or pnl <= 0.0:
                keep = False
                break
            total_pnl += pnl
            total_trades += trade_count
        if not keep:
            continue
        if strategy_name == "structure_core":
            baseline_total_pnl = total_pnl
        ranked.append(
            {
                "strategy_name": strategy_name,
                "filters": filters,
                "total_realized_pnl_usd": total_pnl,
                "total_trade_count": total_trades,
                "avg_realized_pnl_usd": total_pnl / total_trades if total_trades > 0 else 0.0,
                "min_day_pnl_usd": min(float(stats["total_realized_pnl_usd"]) for stats in day_stats.values()),
                "day_stats": day_stats,
                "exit_config_name": f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}",
                "delta_vs_structure_core": None,
            }
        )

    if baseline_total_pnl is not None:
        for row in ranked:
            row["delta_vs_structure_core"] = row["total_realized_pnl_usd"] - baseline_total_pnl
    ranked.sort(key=lambda item: (item["min_day_pnl_usd"], item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    return ranked


def run_dislocation_strategy_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    output_dir: Path,
    label_prefix: str = "dislocation_matrix_15m",
    poly_fee_bps: float = 20.0,
    cex_taker_fee_bps: float = 5.0,
    cex_slippage_bps: float = 2.0,
    exit_config: AggressiveExitConfig | None = None,
    min_trades_per_day: int = 1,
) -> Path:
    fixed_exit_config = exit_config or AggressiveExitConfig("15m", 1, "any_touch", "raw_tick")
    ranked = rank_dislocation_strategy_matrix(
        frames_by_label=frames_by_label,
        poly_fee_bps=poly_fee_bps,
        cex_taker_fee_bps=cex_taker_fee_bps,
        cex_slippage_bps=cex_slippage_bps,
        exit_config=fixed_exit_config,
        min_trades_per_day=min_trades_per_day,
    )
    payload = {
        "labels": list(frames_by_label.keys()),
        "min_trades_per_day": min_trades_per_day,
        "exit_config_name": f"{fixed_exit_config.horizon_label}:{fixed_exit_config.touch_mode}:{fixed_exit_config.target_mode}:ticks{fixed_exit_config.target_ticks}",
        "ranked_strategies": ranked,
    }
    return write_checkpoint(output_dir, "dislocation_strategy_matrix", label_prefix, payload)


def build_dislocation_continuation_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    mask_map = dict(build_aggressive_rule_masks(frame))
    structure_core = (
        mask_map["setup=directional_dislocation"]
        & mask_map["disp>=1500"]
        & mask_map["refill<=0.35"]
    )
    built: list[dict[str, Any]] = []
    for (_, _), group in frame.groupby(["symbol", "ts_ms"], sort=False):
        if group.empty:
            continue
        trigger_indices = list(group.index[group.index.isin(frame.index[structure_core])])
        if not trigger_indices:
            continue
        yes_rows = group.loc[group["claim_side"] == "yes"]
        no_rows = group.loc[group["claim_side"] == "no"]
        if yes_rows.empty or no_rows.empty:
            continue
        for trigger_index in trigger_indices:
            trigger = group.loc[trigger_index]
            trigger_side = str(trigger["claim_side"])
            opposite_rows = yes_rows if trigger_side == "no" else no_rows
            if opposite_rows.empty:
                continue
            complement_gap = (opposite_rows["entry_price"] + float(trigger["entry_price"]) - 1.0).abs()
            execution_index = complement_gap.idxmin()
            execution = opposite_rows.loc[execution_index]
            payload = trigger.to_dict()
            payload["execution_claim_side"] = str(execution["claim_side"])
            payload["execution_entry_price"] = float(execution["entry_price"])
            payload["execution_min_tick_size"] = float(execution["min_tick_size"])
            payload["execution_filled_notional_usd"] = float(execution["filled_notional_usd"])
            payload["execution_complement_gap_abs"] = float(complement_gap.loc[execution_index])
            built.append(payload)
    return pd.DataFrame(built)


def attach_dislocation_continuation_outcomes(
    *,
    frame: pd.DataFrame,
    tape_dir: Path,
    horizon_labels: Iterable[str] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    required_keys = {
        (str(symbol), str(side))
        for symbol, side in zip(frame["symbol"], frame["execution_claim_side"], strict=True)
    }
    side_series = build_poly_side_price_series(tape_dir, allowed_keys=required_keys)
    horizon_map = aggressive_horizon_map()
    selected_horizons = list(horizon_labels) if horizon_labels is not None else list(horizon_map.keys())
    outcome_map = {
        key: {
            horizon_label: compute_horizon_price_outcomes(
                times_ms=list(series.times_ms),
                best_bids=list(series.best_bids),
                last_trades=list(series.last_trades),
                horizon_ms=horizon_map[horizon_label],
            )
            for horizon_label in selected_horizons
        }
        for key, series in side_series.items()
    }

    augmented = frame.copy()
    for horizon_label in selected_horizons:
        augmented[f"execution_outcome_future_max_bid_{horizon_label}"] = 0.0
        augmented[f"execution_outcome_future_max_touch_{horizon_label}"] = 0.0
        augmented[f"execution_outcome_deadline_bid_{horizon_label}"] = 0.0

    ordered = augmented.sort_values(["symbol", "execution_claim_side", "ts_ms"])
    pointer_by_key = {key: -1 for key in side_series}
    for index, row in ordered.iterrows():
        key = (str(row["symbol"]), str(row["execution_claim_side"]))
        series = side_series.get(key)
        outcomes = outcome_map.get(key)
        if series is None or outcomes is None:
            continue
        position = pointer_by_key.get(key, -1)
        times_ms = series.times_ms
        ts_ms = int(row["ts_ms"])
        while position + 1 < len(times_ms) and times_ms[position + 1] <= ts_ms:
            position += 1
        pointer_by_key[key] = position
        if position < 0:
            continue
        for horizon_label, horizon_outcome in outcomes.items():
            augmented.at[index, f"execution_outcome_future_max_bid_{horizon_label}"] = horizon_outcome.future_max_bid[position]
            augmented.at[index, f"execution_outcome_future_max_touch_{horizon_label}"] = horizon_outcome.future_max_touch[position]
            augmented.at[index, f"execution_outcome_deadline_bid_{horizon_label}"] = horizon_outcome.deadline_bid[position]
    return augmented


def load_dislocation_continuation_frame(
    *,
    feature_path: Path,
    tape_dir: Path,
    horizon_labels: Iterable[str] | None = None,
) -> pd.DataFrame:
    grouped_rows: list[pd.DataFrame] = []
    current_key: tuple[str, int] | None = None
    pending_rows: list[dict[str, Any]] = []

    def flush_pending(rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        group_frame = pd.DataFrame(rows)
        paired = build_dislocation_continuation_candidates(group_frame)
        if not paired.empty:
            grouped_rows.append(paired)

    for row in iter_jsonl_zst(feature_path):
        key = (str(row["symbol"]), int(row["ts_ms"]))
        if current_key is None:
            current_key = key
        if key != current_key:
            flush_pending(pending_rows)
            pending_rows = []
            current_key = key
        pending_rows.append(row)
    flush_pending(pending_rows)

    if not grouped_rows:
        return pd.DataFrame()
    paired = pd.concat(grouped_rows, ignore_index=True)
    return attach_dislocation_continuation_outcomes(
        frame=paired,
        tape_dir=tape_dir,
        horizon_labels=horizon_labels,
    )


def build_dislocation_continuation_strategy_masks(frame: pd.DataFrame) -> list[tuple[str, list[str], pd.Series]]:
    if frame.empty:
        return []

    base_mask = pd.Series([True] * len(frame), index=frame.index)
    confirm_window_complete_1s = _frame_column(frame, "confirm_window_complete_1s", False).astype(bool)
    confirm_window_complete_3s = _frame_column(frame, "confirm_window_complete_3s", False).astype(bool)
    refill_1s = pd.to_numeric(_frame_column(frame, "post_pulse_refill_ratio_1s", float("nan")), errors="coerce")
    refill_3s = pd.to_numeric(_frame_column(frame, "post_pulse_refill_ratio_3s", float("nan")), errors="coerce")
    spread_snapback = pd.to_numeric(_frame_column(frame, "spread_snapback_bps", float("nan")), errors="coerce")
    depth_rebuild = pd.to_numeric(_frame_column(frame, "bid_depth_rebuild_ratio", float("nan")), errors="coerce")
    extra_masks: dict[str, pd.Series] = {
        "refill1s_weak": confirm_window_complete_1s & (refill_1s <= 0.10),
        "refill3s_weak": confirm_window_complete_3s & (refill_3s <= 0.15),
        "spread_worse": confirm_window_complete_3s & (spread_snapback <= 0.0),
        "depth_stalled": confirm_window_complete_3s & (depth_rebuild <= 1.00),
    }
    strategy_specs = [
        ("structure_core", ["structure_core"], []),
        ("structure_core + refill1s_weak", ["structure_core", "refill1s_weak"], ["refill1s_weak"]),
        ("structure_core + refill3s_weak", ["structure_core", "refill3s_weak"], ["refill3s_weak"]),
        ("structure_core + spread_worse", ["structure_core", "spread_worse"], ["spread_worse"]),
        ("structure_core + depth_stalled", ["structure_core", "depth_stalled"], ["depth_stalled"]),
        (
            "structure_core + refill3s_weak + spread_worse",
            ["structure_core", "refill3s_weak", "spread_worse"],
            ["refill3s_weak", "spread_worse"],
        ),
        (
            "structure_core + refill3s_weak + depth_stalled",
            ["structure_core", "refill3s_weak", "depth_stalled"],
            ["refill3s_weak", "depth_stalled"],
        ),
        (
            "structure_core + spread_worse + depth_stalled",
            ["structure_core", "spread_worse", "depth_stalled"],
            ["spread_worse", "depth_stalled"],
        ),
        (
            "structure_core + refill3s_weak + spread_worse + depth_stalled",
            ["structure_core", "refill3s_weak", "spread_worse", "depth_stalled"],
            ["refill3s_weak", "spread_worse", "depth_stalled"],
        ),
    ]
    built: list[tuple[str, list[str], pd.Series]] = []
    for strategy_name, filters, extra_names in strategy_specs:
        mask = base_mask.copy()
        for extra_name in extra_names:
            mask = mask & extra_masks[extra_name]
        built.append((strategy_name, filters, mask))
    return built


def derive_dislocation_continuation_exit_frame(
    *,
    frame: pd.DataFrame,
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
) -> pd.DataFrame:
    derived = frame.copy()
    derived["target_exit_price"] = derived["execution_entry_price"] + derived["execution_min_tick_size"] * exit_config.target_ticks

    if exit_config.touch_mode == "bid_touch":
        touch_column = f"execution_outcome_future_max_bid_{exit_config.horizon_label}"
    else:
        touch_column = f"execution_outcome_future_max_touch_{exit_config.horizon_label}"
    deadline_column = f"execution_outcome_deadline_bid_{exit_config.horizon_label}"

    derived["touch_price"] = derived[touch_column]
    derived["deadline_exit_price"] = derived[deadline_column]
    derived["hit_target"] = derived["touch_price"] >= derived["target_exit_price"]
    derived["realized_exit_price"] = derived["deadline_exit_price"].where(
        ~derived["hit_target"],
        derived["target_exit_price"],
    )

    fee_bps = poly_fee_bps + cex_taker_fee_bps
    reserve_bps = cex_taker_fee_bps + poly_fee_bps / 2.0
    net_edge_bps = (
        (derived["realized_exit_price"] - derived["execution_entry_price"]) / derived["execution_entry_price"] * 10_000.0
        - cex_slippage_bps
        - fee_bps
        - reserve_bps
    )
    derived["realized_net_pnl_usd"] = derived["execution_filled_notional_usd"] * net_edge_bps / 10_000.0
    derived["realized_hit"] = derived["hit_target"].astype(int)
    derived["execution_mode"] = "complementary_claim_long"
    derived["exit_config_name"] = (
        f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}"
    )
    return derived


def rank_dislocation_continuation_strategy_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_trades_per_day: int = 1,
) -> list[dict[str, Any]]:
    if not frames_by_label:
        return []

    derived_by_label: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_label.items():
        if frame.empty:
            return []
        derived_by_label[label] = derive_dislocation_continuation_exit_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
        )

    ordered_labels = list(frames_by_label.keys())
    cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
    ranked: list[dict[str, Any]] = []
    baseline_total_pnl: float | None = None
    for strategy_name, filters, _ in build_dislocation_continuation_strategy_masks(derived_by_label[ordered_labels[0]]):
        total_pnl = 0.0
        total_trades = 0
        keep = True
        day_stats: dict[str, dict[str, float | int]] = {}
        for label in ordered_labels:
            derived = derived_by_label[label]
            strategy_mask_map = {name: mask for name, _, mask in build_dislocation_continuation_strategy_masks(derived)}
            subset = apply_rule_cooldown(derived.loc[strategy_mask_map[strategy_name]], cooldown_ms)
            trade_count = int(len(subset))
            pnl = float(subset["realized_net_pnl_usd"].sum()) if trade_count > 0 else 0.0
            hit_rate = float(subset["realized_hit"].mean()) if trade_count > 0 else 0.0
            day_stats[label] = {
                "trade_count": trade_count,
                "total_realized_pnl_usd": pnl,
                "hit_rate": hit_rate,
            }
            if trade_count < min_trades_per_day or pnl <= 0.0:
                keep = False
                break
            total_pnl += pnl
            total_trades += trade_count
        if not keep:
            continue
        if strategy_name == "structure_core":
            baseline_total_pnl = total_pnl
        ranked.append(
            {
                "strategy_name": strategy_name,
                "filters": filters,
                "total_realized_pnl_usd": total_pnl,
                "total_trade_count": total_trades,
                "avg_realized_pnl_usd": total_pnl / total_trades if total_trades > 0 else 0.0,
                "min_day_pnl_usd": min(float(stats["total_realized_pnl_usd"]) for stats in day_stats.values()),
                "day_stats": day_stats,
                "execution_mode": "complementary_claim_long",
                "exit_config_name": f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}",
                "delta_vs_structure_core": None,
            }
        )

    if baseline_total_pnl is not None:
        for row in ranked:
            row["delta_vs_structure_core"] = row["total_realized_pnl_usd"] - baseline_total_pnl
    ranked.sort(key=lambda item: (item["min_day_pnl_usd"], item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    return ranked


def run_dislocation_continuation_strategy_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    output_dir: Path,
    label_prefix: str = "dislocation_continuation_15m",
    poly_fee_bps: float = 20.0,
    cex_taker_fee_bps: float = 5.0,
    cex_slippage_bps: float = 2.0,
    exit_config: AggressiveExitConfig | None = None,
    min_trades_per_day: int = 1,
) -> Path:
    fixed_exit_config = exit_config or AggressiveExitConfig("15m", 1, "any_touch", "raw_tick")
    ranked = rank_dislocation_continuation_strategy_matrix(
        frames_by_label=frames_by_label,
        poly_fee_bps=poly_fee_bps,
        cex_taker_fee_bps=cex_taker_fee_bps,
        cex_slippage_bps=cex_slippage_bps,
        exit_config=fixed_exit_config,
        min_trades_per_day=min_trades_per_day,
    )
    payload = {
        "labels": list(frames_by_label.keys()),
        "execution_mode": "complementary_claim_long",
        "min_trades_per_day": min_trades_per_day,
        "exit_config_name": f"{fixed_exit_config.horizon_label}:{fixed_exit_config.touch_mode}:{fixed_exit_config.target_mode}:ticks{fixed_exit_config.target_ticks}",
        "ranked_strategies": ranked,
    }
    return write_checkpoint(output_dir, "dislocation_continuation_matrix", label_prefix, payload)


def extract_dislocation_continuation_pairs(
    *,
    feature_path: Path,
    tape_dir: Path,
    output_dir: Path,
    label_prefix: str = "dislocation_continuation_pairs",
    horizon_label: str = "15m",
) -> Path:
    paired = load_dislocation_continuation_frame(
        feature_path=feature_path,
        tape_dir=tape_dir,
        horizon_labels=[horizon_label],
    )
    features_dir = output_dir / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    pair_path = features_dir / f"pulse-dislocation-continuation-pairs-{label_prefix}-{stamp}.jsonl.zst"
    writer = subprocess.Popen(["zstd", "-T0", "-q", "-f", "-o", str(pair_path)], stdin=subprocess.PIPE, text=True)
    assert writer.stdin is not None
    try:
        for row in paired.to_dict("records"):
            writer.stdin.write(json.dumps(row, ensure_ascii=False) + "\n")
    finally:
        writer.stdin.close()
        code = writer.wait()
        if code != 0:
            raise RuntimeError(f"zstd writer failed with code {code}")

    side_counts = paired["execution_claim_side"].value_counts().to_dict() if not paired.empty else {}
    payload = {
        "feature_path": str(feature_path),
        "tape_dir": str(tape_dir),
        "horizon_label": horizon_label,
        "rows_written": int(len(paired)),
        "execution_side_counts": side_counts,
        "pair_path": str(pair_path),
    }
    return write_checkpoint(output_dir, "dislocation_continuation_pairs", label_prefix, payload)


def parse_labeled_path_specs(values: list[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"expected label=path, got `{value}`")
        label, raw_path = value.split("=", 1)
        label = label.strip()
        raw_path = raw_path.strip()
        if not label or not raw_path:
            raise SystemExit(f"expected label=path, got `{value}`")
        parsed[label] = Path(raw_path)
    return parsed


def load_dislocation_matrix_frames(
    *,
    outcome_paths_by_label: dict[str, Path],
    catalog: dict[str, MarketSpec] | None = None,
) -> dict[str, pd.DataFrame]:
    frames_by_label: dict[str, pd.DataFrame] = {}
    for label, path in outcome_paths_by_label.items():
        frame = pd.DataFrame(list(iter_jsonl_zst(path)))
        if catalog is not None:
            frame = enrich_frame_with_market_context(frame, catalog)
        frames_by_label[label] = frame
    return frames_by_label


def load_jsonl_frames(paths_by_label: dict[str, Path]) -> dict[str, pd.DataFrame]:
    return {
        label: pd.DataFrame(list(iter_jsonl_zst(path)))
        for label, path in paths_by_label.items()
    }


def rank_cross_day_rules(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_trades_per_day: int = 1,
    top_k: int = 20,
) -> list[dict[str, Any]]:
    if not frames_by_label:
        return []

    derived_by_label: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_label.items():
        if frame.empty:
            return []
        derived_by_label[label] = derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
        )

    ordered_labels = list(frames_by_label.keys())
    base_masks = build_aggressive_rule_masks(derived_by_label[ordered_labels[0]])
    preferred_names = [
        "setup=directional_dislocation",
        "rule=directional",
        "rule=between",
        "entry<=0.15",
        "tts>=24h",
        "basis>=300bps",
        "anchor_follow<=0.10",
        "anchor_follow<=0.25",
        "refill<=0.35",
        "disp>=1500",
        "fair_move<=25",
        "cex_move<=25",
        "extreme_age<=3000",
        "spread<=150bps",
        "spread<=400bps",
    ]
    base_mask_map = dict(base_masks)
    filtered_masks = [(name, base_mask_map[name]) for name in preferred_names if name in base_mask_map]
    rule_specs: list[tuple[str, list[str]]] = [(name, [name]) for name, _ in filtered_masks]
    for idx, (left_name, _) in enumerate(filtered_masks):
        for right_name, _ in filtered_masks[idx + 1 :]:
            rule_specs.append((f"{left_name} & {right_name}", [left_name, right_name]))

    cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
    ranked: list[dict[str, Any]] = []
    for rule_name, rule_parts in rule_specs:
        day_stats: dict[str, dict[str, float | int]] = {}
        keep = True
        total_pnl = 0.0
        total_trades = 0
        for label in ordered_labels:
            derived = derived_by_label[label]
            mask_map = dict(build_aggressive_rule_masks(derived))
            combined = pd.Series([True] * len(derived), index=derived.index)
            for part in rule_parts:
                combined = combined & mask_map[part]
            subset = apply_rule_cooldown(derived.loc[combined], cooldown_ms)
            trade_count = int(len(subset))
            pnl = float(subset["realized_net_pnl_usd"].sum()) if trade_count > 0 else 0.0
            hit_rate = float(subset["realized_hit"].mean()) if trade_count > 0 else 0.0
            day_stats[label] = {
                "trade_count": trade_count,
                "total_realized_pnl_usd": pnl,
                "hit_rate": hit_rate,
            }
            if trade_count < min_trades_per_day or pnl <= 0.0:
                keep = False
                break
            total_pnl += pnl
            total_trades += trade_count
        if not keep:
            continue
        ranked.append(
            {
                "rule_name": rule_name,
                "total_realized_pnl_usd": total_pnl,
                "total_trade_count": total_trades,
                "avg_realized_pnl_usd": total_pnl / total_trades if total_trades > 0 else 0.0,
                "min_day_pnl_usd": min(float(stats["total_realized_pnl_usd"]) for stats in day_stats.values()),
                "day_stats": day_stats,
                "exit_config_name": f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}",
            }
        )
    ranked.sort(key=lambda item: (item["min_day_pnl_usd"], item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    return ranked[:top_k]


def aggressive_horizon_map() -> dict[str, int]:
    return {
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "60m": 60 * 60 * 1000,
    }


def candidate_aggressive_exit_configs() -> list[AggressiveExitConfig]:
    return [
        AggressiveExitConfig(
            horizon_label=horizon_label,
            target_ticks=target_ticks,
            touch_mode=touch_mode,
            target_mode=target_mode,
        )
        for horizon_label in aggressive_horizon_map()
        for target_ticks in (1, 2, 3, 4, 5)
        for touch_mode in ("bid_touch", "any_touch")
        for target_mode in ("raw_tick", "cap_window", "cap_window_fair")
    ]


def build_poly_side_price_series(
    tape_dir: Path,
    *,
    allowed_keys: set[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], SidePriceSeries]:
    books: dict[tuple[str, str], OrderBook] = {}
    series_by_key: dict[tuple[str, str], SidePriceSeries] = {}
    for path in sorted(tape_dir.glob("poly_book-*.jsonl.zst")):
        for row in iter_jsonl_zst(path):
            symbol = str(row["symbol"])
            side = "yes" if row["token_side"] == "yes" else "no"
            key = (symbol, side)
            if allowed_keys is not None and key not in allowed_keys:
                continue
            book = books.get(key)
            if book is None:
                book = OrderBook(bids={}, asks={}, ts_ms=0, received_at_ms=0, sequence=0)
                books[key] = book
            book.ts_ms = int(row["ts_exchange_ms"])
            book.received_at_ms = int(row["ts_recv_ms"])
            book.sequence = int(row["sequence"] or 0)
            payload = row["payload"]
            book.apply(payload.get("bids", []), payload.get("asks", []), row.get("record_kind"))
            best_bid = book.best_bid() or 0.0
            last_trade_raw = payload.get("last_trade_price")
            last_trade = float(last_trade_raw) if last_trade_raw not in (None, "") else 0.0
            series_by_key.setdefault(key, SidePriceSeries.empty()).append(
                int(row["ts_recv_ms"]),
                best_bid,
                last_trade,
            )
    return series_by_key


def extract_aggressive_outcomes(
    *,
    tape_dir: Path,
    feature_path: Path,
    output_dir: Path,
    label_prefix: str = "aggressive_outcomes",
) -> Path:
    horizon_map = aggressive_horizon_map()
    side_series = build_poly_side_price_series(tape_dir)
    outcome_map = {
        key: {
            horizon_label: compute_horizon_price_outcomes(
                times_ms=list(series.times_ms),
                best_bids=list(series.best_bids),
                last_trades=list(series.last_trades),
                horizon_ms=horizon_ms,
            )
            for horizon_label, horizon_ms in horizon_map.items()
        }
        for key, series in side_series.items()
    }

    features_dir = output_dir / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outcome_path = features_dir / f"pulse-aggressive-outcomes-{label_prefix}-{stamp}.jsonl.zst"
    writer = subprocess.Popen(["zstd", "-T0", "-q", "-f", "-o", str(outcome_path)], stdin=subprocess.PIPE, text=True)
    assert writer.stdin is not None

    pointer_by_key = {key: -1 for key in side_series}
    stats = {
        "feature_path": str(feature_path),
        "rows_seen": 0,
        "rows_written": 0,
        "rows_by_asset": {"btc": 0, "eth": 0},
        "rows_by_side": {"yes": 0, "no": 0},
        "missing_side_series": 0,
    }
    try:
        for row in iter_feature_rows(feature_path):
            stats["rows_seen"] += 1
            if not row.flow_shape_valid:
                continue
            key = (row.symbol, row.claim_side)
            series = side_series.get(key)
            if series is None:
                stats["missing_side_series"] += 1
                continue
            position = pointer_by_key[key]
            times_ms = series.times_ms
            while position + 1 < len(times_ms) and times_ms[position + 1] <= row.ts_ms:
                position += 1
            pointer_by_key[key] = position
            if position < 0:
                stats["missing_side_series"] += 1
                continue

            payload = asdict(row)
            for horizon_label, horizon_outcome in outcome_map[key].items():
                payload[f"outcome_future_max_bid_{horizon_label}"] = horizon_outcome.future_max_bid[position]
                payload[f"outcome_future_max_touch_{horizon_label}"] = horizon_outcome.future_max_touch[position]
                payload[f"outcome_deadline_bid_{horizon_label}"] = horizon_outcome.deadline_bid[position]
            writer.stdin.write(json.dumps(payload, ensure_ascii=False) + "\n")
            stats["rows_written"] += 1
            stats["rows_by_asset"][row.asset] += 1
            stats["rows_by_side"][row.claim_side] += 1
    finally:
        writer.stdin.close()
        code = writer.wait()
        if code != 0:
            raise RuntimeError(f"zstd writer failed with code {code}")

    return write_checkpoint(output_dir, "aggressive_outcomes", label_prefix, {**stats, "outcome_path": str(outcome_path)})


def search_aggressive_profitable_signals(
    *,
    outcome_path: Path,
    params: RuntimeParams,
    output_dir: Path,
    min_trades: int = 25,
    top_k: int = 25,
    label_prefix: str = "aggressive_search",
) -> Path:
    frame = pd.DataFrame(list(iter_jsonl_zst(outcome_path)))
    if frame.empty:
        return write_checkpoint(
            output_dir,
            "aggressive_search",
            label_prefix,
            {"outcome_path": str(outcome_path), "row_count": 0, "top_rules": [], "best_rule": None},
        )

    top_exit_configs = rank_exit_config_baselines(
        frame=frame,
        poly_fee_bps=params.poly_fee_bps,
        cex_taker_fee_bps=params.cex_taker_fee_bps,
        cex_slippage_bps=params.cex_slippage_bps,
        exit_configs=candidate_aggressive_exit_configs(),
        top_k=min(8, top_k),
    )
    ranked_results: list[dict[str, Any]] = []
    top_exit_names = {item["exit_config_name"] for item in top_exit_configs}
    for exit_config in candidate_aggressive_exit_configs():
        exit_config_name = f"{exit_config.horizon_label}:{exit_config.touch_mode}:{exit_config.target_mode}:ticks{exit_config.target_ticks}"
        if exit_config_name not in top_exit_names:
            continue
        derived = derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=params.poly_fee_bps,
            cex_taker_fee_bps=params.cex_taker_fee_bps,
            cex_slippage_bps=params.cex_slippage_bps,
            exit_config=exit_config,
        )
        cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
        for row in rank_aggressive_signal_rules(
            frame=derived,
            min_trades=min_trades,
            top_k=top_k,
            cooldown_ms=cooldown_ms,
        ):
            ranked_results.append(row)

    ranked_results.sort(key=lambda item: (item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    best_rule = ranked_results[0] if ranked_results else None
    payload = {
        "outcome_path": str(outcome_path),
        "row_count": int(len(frame)),
        "min_trades": min_trades,
        "top_exit_configs": top_exit_configs,
        "best_rule": best_rule,
        "top_rules": ranked_results[:top_k],
    }
    return write_checkpoint(output_dir, "aggressive_search", label_prefix, payload)


def rank_exit_config_baselines(
    *,
    frame: pd.DataFrame,
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_configs: list[AggressiveExitConfig],
    top_k: int,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for exit_config in exit_configs:
        derived = derive_aggressive_exit_frame(
            frame=frame,
            poly_fee_bps=poly_fee_bps,
            cex_taker_fee_bps=cex_taker_fee_bps,
            cex_slippage_bps=cex_slippage_bps,
            exit_config=exit_config,
        )
        cooldown_ms = aggressive_horizon_map()[exit_config.horizon_label]
        cooled = apply_rule_cooldown(derived, cooldown_ms)
        ranked.append(
            {
                "exit_config_name": str(cooled["exit_config_name"].iloc[0]),
                "trade_count": int(len(cooled)),
                "total_realized_pnl_usd": float(cooled["realized_net_pnl_usd"].sum()),
                "avg_realized_pnl_usd": float(cooled["realized_net_pnl_usd"].mean()) if len(cooled) > 0 else 0.0,
                "hit_rate": float(cooled["realized_hit"].mean()) if len(cooled) > 0 else 0.0,
            }
        )
    ranked.sort(key=lambda item: (item["total_realized_pnl_usd"], item["avg_realized_pnl_usd"]), reverse=True)
    return ranked[:top_k]


def build_research_feature_row(
    *,
    params: RuntimeParams,
    market: MarketSpec,
    claim_side: str,
    claim_book: OrderBook,
    cex_book: OrderBook,
    market_tape: SignalTape,
    asset_cex_tape: SignalTape,
    fair_prob_yes: float,
    anchor: AnchorSnapshot,
    now_ms: int,
    signal_mode: str = "flow-first",
    confirmation_samples: list[ConfirmationSample] | None = None,
) -> tuple[ResearchFeatureRow | None, str | None]:
    if now_ms - claim_book.received_at_ms > params.poly_open_max_quote_age_ms:
        return None, "poly_quote_stale"
    if now_ms - cex_book.received_at_ms > params.cex_open_max_quote_age_ms:
        return None, "cex_quote_stale"
    anchor_age_ms = now_ms - anchor.latest_received_at_ms
    anchor_latency_delta_ms = now_ms - anchor.ts_ms
    if anchor_age_ms > params.max_anchor_age_ms:
        return None, "anchor_age_rejected"
    if anchor_latency_delta_ms > params.max_anchor_latency_delta_ms:
        return None, "anchor_latency_delta_rejected"

    entry = claim_book.executable_buy_notional(params.opening_request_notional_usd)
    if entry["filled_notional_usd"] <= 0.0 or entry["avg_price"] <= 0.0:
        return None, "no_entry_depth"

    claim_series = "yes_ask" if claim_side == "yes" else "no_ask"
    claim_window = market_tape.window_decimal_snapshot(claim_series, claim_book.ts_ms, params.pulse_window_ms)
    fair_window = market_tape.window_float_snapshot("fair_prob_yes", now_ms, params.pulse_window_ms)
    cex_window = asset_cex_tape.window_decimal_snapshot("cex_mid", cex_book.ts_ms, params.pulse_window_ms)
    if claim_window is None or fair_window is None or cex_window is None:
        return None, "missing_pulse_history"

    current_bid = claim_book.best_bid() or 0.0
    current_ask = claim_book.best_ask() or entry["avg_price"]
    current_mid = claim_book.mid() or entry["avg_price"]
    fair_claim_price = fair_prob_yes if claim_side == "yes" else (1.0 - fair_prob_yes)
    hours_to_settlement = max(market.settlement_ts_ms - now_ms, 0) / (60.0 * 60.0 * 1000.0)
    flow_metrics = compute_flow_reversion_metrics(
        window=claim_window,
        current_value=current_ask,
        now_ms=claim_book.ts_ms,
    )
    confirmation_metrics = None
    if flow_metrics is not None and confirmation_samples:
        confirmation_metrics = compute_post_pulse_confirmation_metrics(
            samples=confirmation_samples,
            window_high_price=claim_window.max_value,
            window_low_price=claim_window.min_value,
            window_low_ts_ms=claim_window.min_ts_ms,
            now_ts_ms=now_ms,
        )
    window_displacement = ((claim_window.max_value - claim_window.min_value) / claim_window.max_value) * 10_000.0 if claim_window.max_value > 0.0 else 0.0
    fair_move = range_move_bps(fair_window)
    return ResearchFeatureRow(
        ts_ms=now_ms,
        asset=market.asset,
        symbol=market.symbol,
        signal_mode=signal_mode,
        claim_side=claim_side,
        entry_price=entry["avg_price"],
        current_bid=current_bid,
        current_ask=current_ask,
        current_mid=current_mid,
        filled_notional_usd=entry["filled_notional_usd"],
        filled_shares=entry["filled_shares"],
        levels_consumed=entry["levels_consumed"],
        depth_fill_ratio=entry["depth_fill_ratio"],
        min_tick_size=market.min_tick_size,
        fair_prob_yes=fair_prob_yes,
        fair_claim_price=fair_claim_price,
        anchor_age_ms=anchor_age_ms,
        anchor_latency_delta_ms=anchor_latency_delta_ms,
        window_first_price=claim_window.first_value,
        window_last_price=claim_window.last_value,
        window_high_price=claim_window.max_value,
        window_low_price=claim_window.min_value,
        window_high_ts_ms=claim_window.max_ts_ms,
        window_low_ts_ms=claim_window.min_ts_ms,
        claim_move_bps=((claim_window.max_value - entry["avg_price"]) / claim_window.max_value) * 10_000.0
        if claim_window.max_value > entry["avg_price"] > 0.0
        else 0.0,
        window_displacement_bps=window_displacement,
        flow_refill_ratio=flow_metrics.refill_ratio if flow_metrics else 0.0,
        flow_rebound_bps=flow_metrics.rebound_from_low_bps if flow_metrics else 0.0,
        flow_residual_gap_bps=flow_metrics.residual_gap_bps if flow_metrics else 0.0,
        flow_extreme_age_ms=flow_metrics.extreme_age_ms if flow_metrics else 0,
        flow_shape_valid=flow_metrics is not None,
        fair_claim_move_bps=fair_move,
        cex_mid_move_bps=range_move_bps(cex_window),
        timeout_exit_price=claim_book.executable_sell_shares(entry["filled_shares"]) or 0.0,
        residual_basis_bps=residual_basis_bps(entry_price=entry["avg_price"], fair_claim_price=fair_claim_price),
        anchor_follow_ratio=anchor_follow_ratio(window_displacement_bps=window_displacement, fair_claim_move_bps=fair_move),
        market_rule_kind=market.rule.kind,
        market_family=market_family_for_rule(market.rule.kind),
        entry_price_bucket=entry_price_bucket(entry["avg_price"]),
        time_to_settlement_hours=hours_to_settlement,
        time_to_settlement_bucket=time_to_settlement_bucket(hours_to_settlement),
        post_pulse_refill_ratio_1s=confirmation_metrics.post_pulse_refill_ratio_1s if confirmation_metrics else None,
        post_pulse_refill_ratio_3s=confirmation_metrics.post_pulse_refill_ratio_3s if confirmation_metrics else None,
        spread_snapback_bps=confirmation_metrics.spread_snapback_bps if confirmation_metrics else None,
        bid_depth_rebuild_ratio=confirmation_metrics.bid_depth_rebuild_ratio if confirmation_metrics else None,
        confirm_window_complete_1s=confirmation_metrics.confirm_window_complete_1s if confirmation_metrics else False,
        confirm_window_complete_3s=confirmation_metrics.confirm_window_complete_3s if confirmation_metrics else False,
    ), None


def scan_feature_grid(
    rows: Iterable[ResearchFeatureRow],
    *,
    min_expected_net_pnl_usd: float,
    min_net_session_edge_bps: float,
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    param_grid: dict[str, list[float | int]],
) -> dict[str, Any]:
    keys = [
        "min_displacement_bps",
        "min_residual_gap_bps",
        "max_refill_ratio",
        "max_fair_move_bps",
        "max_cex_move_bps",
        "target_ticks",
        "max_timeout_loss_usd",
        "max_required_hit_rate",
    ]
    combos: list[dict[str, Any]] = []
    fee_bps = poly_fee_bps + cex_taker_fee_bps
    reserve_bps = cex_taker_fee_bps + poly_fee_bps / 2.0
    for values in itertools.product(*(param_grid[key] for key in keys)):
        combo = dict(zip(keys, values, strict=True))
        combos.append(
            {
                **combo,
                "candidate_count": 0,
                "total_realizable_ev_usd": 0.0,
                "total_expected_net_pnl_usd": 0.0,
                "total_timeout_net_pnl_usd": 0.0,
            }
        )

    row_count = 0
    flow_shape_valid_count = 0
    asset_counts = {"btc": 0, "eth": 0}
    for row in rows:
        row_count += 1
        if row.asset in asset_counts:
            asset_counts[row.asset] += 1
        if not row.flow_shape_valid:
            continue
        flow_shape_valid_count += 1
        for combo in combos:
            if row.window_displacement_bps < float(combo["min_displacement_bps"]):
                continue
            if row.flow_residual_gap_bps < float(combo["min_residual_gap_bps"]):
                continue
            if row.flow_refill_ratio > float(combo["max_refill_ratio"]):
                continue
            if row.fair_claim_move_bps > float(combo["max_fair_move_bps"]):
                continue
            if row.cex_mid_move_bps > float(combo["max_cex_move_bps"]):
                continue

            target_exit_price = min(
                row.entry_price + row.min_tick_size * int(combo["target_ticks"]),
                max(row.window_high_price, row.entry_price),
                max(row.fair_claim_price, row.entry_price),
                1.0,
            )
            expected_net_edge_bps = price_edge_bps(row.entry_price, target_exit_price) - cex_slippage_bps - fee_bps - reserve_bps
            expected_net_pnl_usd = row.filled_notional_usd * expected_net_edge_bps / 10_000.0
            timeout_net_edge_bps = price_edge_bps(row.entry_price, row.timeout_exit_price) - cex_slippage_bps - fee_bps - reserve_bps
            timeout_net_pnl_usd = row.filled_notional_usd * timeout_net_edge_bps / 10_000.0
            timeout_loss_estimate_usd = max(-timeout_net_pnl_usd, 0.0)
            if expected_net_edge_bps < min_net_session_edge_bps:
                continue
            if expected_net_pnl_usd < min_expected_net_pnl_usd:
                continue
            if timeout_loss_estimate_usd > float(combo["max_timeout_loss_usd"]):
                continue
            required_hit_rate = compute_required_hit_rate(
                expected_net_pnl_usd,
                timeout_loss_estimate_usd,
                min_expected_net_pnl_usd,
            )
            if required_hit_rate > float(combo["max_required_hit_rate"]):
                continue
            target_distance_to_mid_bps = abs(target_exit_price - row.current_mid) / row.current_mid * 10_000.0 if row.current_mid > 0.0 else 0.0
            predicted_hit_rate = estimate_base_hit_rate(
                target_distance_to_mid_bps,
                row.filled_notional_usd,
                row.levels_consumed,
                row.window_displacement_bps,
                row.depth_fill_ratio,
            )
            realizable_ev_usd = predicted_hit_rate * expected_net_pnl_usd + (1.0 - predicted_hit_rate) * timeout_net_pnl_usd
            combo["candidate_count"] += 1
            combo["total_realizable_ev_usd"] += realizable_ev_usd
            combo["total_expected_net_pnl_usd"] += expected_net_pnl_usd
            combo["total_timeout_net_pnl_usd"] += timeout_net_pnl_usd
    combos.sort(key=lambda item: (item["total_realizable_ev_usd"], item["candidate_count"]), reverse=True)
    return {
        "row_count": row_count,
        "flow_shape_valid_count": flow_shape_valid_count,
        "asset_counts": asset_counts,
        "best_combo": combos[0] if combos else None,
        "ranked_results": combos,
    }


def default_feature_param_grid() -> dict[str, list[float | int]]:
    return {
        "min_displacement_bps": [50.0, 150.0, 300.0],
        "min_residual_gap_bps": [25.0, 75.0, 150.0],
        "max_refill_ratio": [0.20, 0.35, 0.50],
        "max_fair_move_bps": [10.0, 25.0],
        "max_cex_move_bps": [10.0, 25.0],
        "target_ticks": [1, 2, 3],
        "max_timeout_loss_usd": [10.0, 20.0],
        "max_required_hit_rate": [0.60, 0.75],
    }


def iter_feature_rows(path: Path) -> Iterable[ResearchFeatureRow]:
    for row in iter_jsonl_zst(path):
        yield ResearchFeatureRow(**row)


def extract_research_features(
    *,
    tape_dir: Path,
    params: RuntimeParams,
    config_path: Path,
    output_dir: Path,
    checkpoint_every_rows: int = 250_000,
    label_prefix: str = "feature_extract",
) -> Path:
    catalog = load_market_catalog(config_path)
    market_tapes: dict[str, SignalTape] = {}
    asset_cex_tapes: dict[str, SignalTape] = {"btc": SignalTape(), "eth": SignalTape()}
    confirmation_tapes: dict[tuple[str, str], ConfirmationTape] = {}
    books: dict[tuple[str, str], OrderBook] = {}
    cex_books: dict[str, OrderBook] = {}
    deribit_surface: dict[str, dict[int, dict[str, dict[str, Any]]]] = {"btc": {}, "eth": {}}
    stats = {
        "events_total": 0,
        "poly_events": 0,
        "binance_events": 0,
        "deribit_events": 0,
        "rows_written": 0,
        "rows_by_asset": {"btc": 0, "eth": 0},
        "rows_by_side": {"yes": 0, "no": 0},
        "rejections": {},
        "flow_shape_valid_rows": 0,
    }
    features_dir = output_dir / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    feature_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    feature_path = features_dir / f"pulse-research-features-{label_prefix}-{feature_stamp}.jsonl.zst"
    proc = subprocess.Popen(
        ["zstd", "-T0", "-q", "-f", "-o", str(feature_path)],
        stdin=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    try:
        for row in merge_event_streams(tape_dir):
            stats["events_total"] += 1
            stream = row["stream"]
            if stream == "deribit_option_ticker":
                stats["deribit_events"] += 1
                _update_deribit_surface(deribit_surface, row)
                continue
            if stream == "binance_book":
                stats["binance_events"] += 1
                asset = str(row["asset"])
                book = OrderBook(
                    bids={},
                    asks={},
                    ts_ms=int(row["ts_exchange_ms"]),
                    received_at_ms=int(row["ts_recv_ms"]),
                    sequence=int(row["sequence"] or 0),
                )
                payload = row["payload"]
                book.apply(payload.get("bids", []), payload.get("asks", []), row.get("record_kind"))
                cex_books[asset] = book
                mid = book.mid()
                if mid is not None:
                    asset_cex_tapes.setdefault(asset, SignalTape()).push_decimal(
                        "cex_mid",
                        ts_ms=book.ts_ms,
                        received_at_ms=book.received_at_ms,
                        sequence=book.sequence,
                        value=mid,
                        retention_ms=params.pulse_window_ms * 4,
                    )
                continue
            if stream != "poly_book":
                continue

            stats["poly_events"] += 1
            symbol = str(row["symbol"])
            side = "yes" if row["token_side"] == "yes" else "no"
            key = (symbol, side)
            book = books.get(key)
            if book is None:
                book = OrderBook(bids={}, asks={}, ts_ms=0, received_at_ms=0, sequence=0)
                books[key] = book
            book.ts_ms = int(row["ts_exchange_ms"])
            book.received_at_ms = int(row["ts_recv_ms"])
            book.sequence = int(row["sequence"] or 0)
            payload = row["payload"]
            book.apply(payload.get("bids", []), payload.get("asks", []), row.get("record_kind"))

            tape = market_tapes.setdefault(symbol, SignalTape())
            ask = book.best_ask()
            if ask is not None:
                tape.push_decimal(
                    f"{side}_ask",
                    ts_ms=book.ts_ms,
                    received_at_ms=book.received_at_ms,
                    sequence=book.sequence,
                    value=ask,
                    retention_ms=params.pulse_window_ms * 4,
                )
                bid = book.best_bid() or ask
                mid = book.mid() or ask
                spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0.0 else 0.0
                confirmation_tapes.setdefault(key, ConfirmationTape()).push(
                    ConfirmationSample(
                        ts_ms=book.ts_ms,
                        price=ask,
                        spread_bps=spread_bps,
                        top3_bid_notional=book.top_bid_notional(3),
                    ),
                    retention_ms=params.pulse_window_ms * 4,
                )

            market = catalog.get(symbol)
            if market is None:
                continue
            asset = market.asset
            yes_book = books.get((symbol, "yes"))
            no_book = books.get((symbol, "no"))
            cex_book = cex_books.get(asset)
            if yes_book is None or no_book is None or cex_book is None:
                continue

            anchor = _anchor_snapshot_for_market(deribit_surface.get(asset, {}), market)
            if anchor is None:
                continue
            now_ms = int(row["ts_recv_ms"])
            try:
                fair_prob_yes = probability_for_rule(
                    anchor=anchor,
                    rule=market.rule,
                    event_expiry_ts_ms=market.settlement_ts_ms,
                    expiry_gap=ExpiryGapAdjustment(60, 360, 720),
                )
            except ValueError:
                _bump_reason(stats["rejections"], "hard_expiry_gap_exceeded")
                continue
            tape.push_float(
                "fair_prob_yes",
                ts_ms=now_ms,
                received_at_ms=now_ms,
                sequence=1,
                value=fair_prob_yes,
                retention_ms=params.pulse_window_ms * 4,
            )

            for claim_side, claim_book in (("yes", yes_book), ("no", no_book)):
                feature_row, reason = build_research_feature_row(
                    params=params,
                    market=market,
                    claim_side=claim_side,
                    claim_book=claim_book,
                    cex_book=cex_book,
                    market_tape=tape,
                    asset_cex_tape=asset_cex_tapes[asset],
                    fair_prob_yes=fair_prob_yes,
                    anchor=anchor,
                    now_ms=now_ms,
                    confirmation_samples=confirmation_tapes.get((symbol, claim_side), ConfirmationTape()).recent_samples(),
                )
                if reason is not None:
                    _bump_reason(stats["rejections"], reason)
                    continue
                if feature_row is None:
                    continue
                proc.stdin.write(json.dumps(asdict(feature_row), ensure_ascii=False) + "\n")
                stats["rows_written"] += 1
                stats["rows_by_asset"][asset] += 1
                stats["rows_by_side"][claim_side] += 1
                if feature_row.flow_shape_valid:
                    stats["flow_shape_valid_rows"] += 1

                if checkpoint_every_rows > 0 and stats["rows_written"] % checkpoint_every_rows == 0:
                    write_checkpoint(
                        output_dir,
                        "feature_extract",
                        f"{label_prefix}-progress",
                        {
                            **stats,
                            "feature_path": str(feature_path),
                        },
                    )
    finally:
        proc.stdin.close()
        code = proc.wait()
        if code != 0:
            raise RuntimeError(f"zstd writer failed with code {code}")

    return write_checkpoint(
        output_dir,
        "feature_extract",
        f"{label_prefix}-final",
        {
            **stats,
            "feature_path": str(feature_path),
        },
    )


def run_feature_matrix(
    *,
    feature_path: Path,
    params: RuntimeParams,
    output_dir: Path,
    label_prefix: str = "matrix",
) -> Path:
    matrix = scan_feature_grid(
        iter_feature_rows(feature_path),
        min_expected_net_pnl_usd=params.min_expected_net_pnl_usd,
        min_net_session_edge_bps=params.min_net_session_edge_bps,
        poly_fee_bps=params.poly_fee_bps,
        cex_taker_fee_bps=params.cex_taker_fee_bps,
        cex_slippage_bps=params.cex_slippage_bps,
        param_grid=default_feature_param_grid(),
    )
    payload = {
        "feature_path": str(feature_path),
        "row_count": matrix["row_count"],
        "flow_shape_valid_count": matrix["flow_shape_valid_count"],
        "asset_counts": matrix["asset_counts"],
        "best_combo": matrix["best_combo"],
        "top_results": matrix["ranked_results"][:20],
    }
    return write_checkpoint(output_dir, "feature_matrix", label_prefix, payload)


def _bump_reason(buckets: dict[str, int], reason: str) -> None:
    buckets[reason] = buckets.get(reason, 0) + 1


def _write_signal_summary_checkpoint(
    output_dir: Path,
    stats: dict[str, Any],
    recent_candidates: list[CandidateRecord],
    *,
    signal_mode: str,
    label: str,
) -> Path:
    payload = {
        "signal_mode": signal_mode,
        "events_total": stats["events_total"],
        "poly_events": stats["poly_events"],
        "binance_events": stats["binance_events"],
        "deribit_events": stats["deribit_events"],
        "markets_seen": len(stats["markets_seen"]),
        "candidate_count": stats["candidate_count"],
        "candidate_by_asset": stats["candidate_by_asset"],
        "candidate_by_side": stats["candidate_by_side"],
        "rejections": stats["rejections"],
        "recent_candidates": [asdict(item) for item in recent_candidates],
    }
    return write_checkpoint(output_dir, "signal_coarse", label, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pulse research sweep checkpoint helper")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--tape-dir", type=Path, default=DEFAULT_TAPE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--stage",
        choices=[
            "bootstrap",
            "signal-coarse",
            "feature-extract",
            "feature-matrix",
            "aggressive-outcomes",
            "aggressive-search",
            "dislocation-strategy-matrix",
            "dislocation-reachability-matrix",
            "geometry-signal-matrix",
            "dislocation-continuation-pairs",
            "dislocation-continuation-matrix",
        ],
        default="bootstrap",
        help="bootstrap 校验入口；signal-coarse 跑粗筛；feature-extract 抽取逐样本研究表；feature-matrix 在研究表上扫矩阵；aggressive-outcomes 生成未来 outcome 标注；aggressive-search 搜样本内最赚钱规则；dislocation-strategy-matrix 评估固定 15m 回补策略矩阵；dislocation-reachability-matrix 评估固定 15m/1tick 下的可达性闸门；geometry-signal-matrix 评估 geometry-gated directional signal 矩阵；dislocation-continuation-pairs 先抽 continuation pairs 缓存；dislocation-continuation-matrix 评估固定 15m continuation 策略矩阵",
    )
    parser.add_argument(
        "--signal-mode",
        choices=["baseline", "flow-first"],
        default="baseline",
        help="baseline 复用现有 runtime-like 语义；flow-first 改用脉冲窗极值和第一口回补生成候选",
    )
    parser.add_argument("--checkpoint-every-poly-events", type=int, default=250_000)
    parser.add_argument("--checkpoint-every-feature-rows", type=int, default=250_000)
    parser.add_argument("--override-min-claim-move-bps", type=float, default=None)
    parser.add_argument("--override-max-anchor-age-ms", type=int, default=None)
    parser.add_argument("--override-min-pulse-score-bps", type=float, default=None)
    parser.add_argument("--feature-path", type=Path, default=None)
    parser.add_argument("--matrix-outcome", action="append", default=None)
    parser.add_argument("--matrix-feature", action="append", default=None)
    parser.add_argument("--matrix-paired", action="append", default=None)
    parser.add_argument("--matrix-tape-dir", action="append", default=None)
    parser.add_argument("--matrix-symbol", action="append", default=None)
    parser.add_argument("--matrix-min-trades-per-day", type=int, default=3)
    parser.add_argument("--output-label", default="baseline")
    args = parser.parse_args()

    if args.stage == "bootstrap":
        catalog = load_market_catalog(args.config)
        symbols_in_tape: set[str] = set()
        for path in sorted(args.tape_dir.glob("poly_book-*.jsonl.zst")):
            for row in iter_jsonl_zst(path):
                symbols_in_tape.add(str(row["symbol"]))

        matched = {symbol: catalog[symbol] for symbol in symbols_in_tape if symbol in catalog}
        by_asset: dict[str, int] = {}
        for spec in matched.values():
            by_asset[spec.asset] = by_asset.get(spec.asset, 0) + 1

        payload = {
            "tape_dir": str(args.tape_dir),
            "symbol_count": len(symbols_in_tape),
            "catalog_match_count": len(matched),
            "asset_counts": by_asset,
            "sample_symbols": sorted(symbols_in_tape)[:12],
        }
        path = write_checkpoint(args.output_dir, args.stage, "bootstrap", payload)
    elif args.stage == "signal-coarse":
        params = load_runtime_params(args.config)
        if args.override_min_claim_move_bps is not None:
            params = replace(params, min_claim_price_move_bps=args.override_min_claim_move_bps)
        if args.override_max_anchor_age_ms is not None:
            params = replace(params, max_anchor_age_ms=args.override_max_anchor_age_ms)
        if args.override_min_pulse_score_bps is not None:
            params = replace(params, min_pulse_score_bps=args.override_min_pulse_score_bps)
        path = analyze_signal_coarse(
            tape_dir=args.tape_dir,
            params=params,
            config_path=args.config,
            output_dir=args.output_dir,
            signal_mode=args.signal_mode,
            checkpoint_every_poly_events=args.checkpoint_every_poly_events,
            label_prefix=args.output_label,
        )
    elif args.stage == "feature-extract":
        params = load_runtime_params(args.config)
        path = extract_research_features(
            tape_dir=args.tape_dir,
            params=params,
            config_path=args.config,
            output_dir=args.output_dir,
            checkpoint_every_rows=args.checkpoint_every_feature_rows,
            label_prefix=args.output_label,
        )
    elif args.stage == "feature-matrix":
        params = load_runtime_params(args.config)
        if args.feature_path is None:
            raise SystemExit("--feature-path is required for feature-matrix stage")
        path = run_feature_matrix(
            feature_path=args.feature_path,
            params=params,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
        )
    elif args.stage == "aggressive-outcomes":
        if args.feature_path is None:
            raise SystemExit("--feature-path is required for aggressive-outcomes stage")
        path = extract_aggressive_outcomes(
            tape_dir=args.tape_dir,
            feature_path=args.feature_path,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
        )
    elif args.stage == "aggressive-search":
        params = load_runtime_params(args.config)
        if args.feature_path is None:
            raise SystemExit("--feature-path is required for aggressive-search stage")
        path = search_aggressive_profitable_signals(
            outcome_path=args.feature_path,
            params=params,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
        )
    elif args.stage == "dislocation-strategy-matrix":
        if not args.matrix_outcome:
            raise SystemExit("--matrix-outcome is required for dislocation-strategy-matrix stage")
        params = load_runtime_params(args.config)
        catalog = load_market_catalog(args.config)
        frames_by_label = load_dislocation_matrix_frames(
            outcome_paths_by_label=parse_labeled_path_specs(args.matrix_outcome),
            catalog=catalog,
        )
        path = run_dislocation_strategy_matrix(
            frames_by_label=frames_by_label,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
            poly_fee_bps=params.poly_fee_bps,
            cex_taker_fee_bps=params.cex_taker_fee_bps,
            cex_slippage_bps=params.cex_slippage_bps,
            min_trades_per_day=args.matrix_min_trades_per_day,
        )
    elif args.stage == "dislocation-reachability-matrix":
        if not args.matrix_outcome:
            raise SystemExit("--matrix-outcome is required for dislocation-reachability-matrix stage")
        params = load_runtime_params(args.config)
        catalog = load_market_catalog(args.config)
        frames_by_label = load_dislocation_matrix_frames(
            outcome_paths_by_label=parse_labeled_path_specs(args.matrix_outcome),
            catalog=catalog,
        )
        path = run_dislocation_reachability_matrix(
            frames_by_label=frames_by_label,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
            poly_fee_bps=params.poly_fee_bps,
            cex_taker_fee_bps=params.cex_taker_fee_bps,
            cex_slippage_bps=params.cex_slippage_bps,
            min_expected_net_pnl_usd=params.min_expected_net_pnl_usd,
            min_trades_per_day=args.matrix_min_trades_per_day,
        )
    elif args.stage == "geometry-signal-matrix":
        if not args.matrix_outcome:
            raise SystemExit("--matrix-outcome is required for geometry-signal-matrix stage")
        params = load_runtime_params(args.config)
        catalog = load_market_catalog(args.config)
        frames_by_label = load_dislocation_matrix_frames(
            outcome_paths_by_label=parse_labeled_path_specs(args.matrix_outcome),
            catalog=catalog,
        )
        path = run_geometry_signal_matrix(
            frames_by_label=frames_by_label,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
            poly_fee_bps=params.poly_fee_bps,
            cex_taker_fee_bps=params.cex_taker_fee_bps,
            cex_slippage_bps=params.cex_slippage_bps,
            min_expected_net_pnl_usd=params.min_expected_net_pnl_usd,
            min_trades_per_day=args.matrix_min_trades_per_day,
            symbol_filters=set(args.matrix_symbol or []),
        )
    elif args.stage == "dislocation-continuation-pairs":
        if args.feature_path is None:
            raise SystemExit("--feature-path is required for dislocation-continuation-pairs stage")
        path = extract_dislocation_continuation_pairs(
            feature_path=args.feature_path,
            tape_dir=args.tape_dir,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
        )
    else:
        params = load_runtime_params(args.config)
        if args.matrix_paired:
            frames_by_label = load_jsonl_frames(parse_labeled_path_specs(args.matrix_paired))
        else:
            if not args.matrix_feature:
                raise SystemExit("--matrix-feature is required for dislocation-continuation-matrix stage when matrix-paired is absent")
            if not args.matrix_tape_dir:
                raise SystemExit("--matrix-tape-dir is required for dislocation-continuation-matrix stage when matrix-paired is absent")
            feature_paths_by_label = parse_labeled_path_specs(args.matrix_feature)
            tape_dirs_by_label = parse_labeled_path_specs(args.matrix_tape_dir)
            if set(feature_paths_by_label) != set(tape_dirs_by_label):
                raise SystemExit("matrix-feature labels must match matrix-tape-dir labels")
            frames_by_label = {
                label: load_dislocation_continuation_frame(
                    feature_path=feature_path,
                    tape_dir=tape_dirs_by_label[label],
                    horizon_labels=["15m"],
                )
                for label, feature_path in feature_paths_by_label.items()
            }
        path = run_dislocation_continuation_strategy_matrix(
            frames_by_label=frames_by_label,
            output_dir=args.output_dir,
            label_prefix=args.output_label,
            poly_fee_bps=params.poly_fee_bps,
            cex_taker_fee_bps=params.cex_taker_fee_bps,
            cex_slippage_bps=params.cex_slippage_bps,
            min_trades_per_day=args.matrix_min_trades_per_day,
        )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
