from __future__ import annotations

import json
from collections import deque
from pathlib import Path
import unittest

from module_loader import REPO_ROOT, load_script_module


report = load_script_module(
    "btc_basis_backtest_report_fixture",
    "scripts/btc_basis_backtest_report.py",
)

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "btc_price_only_parity_cases.json"


class _Contract:
    def __init__(self, rule: report.MarketRule, token_side: str) -> None:
        self.rule = rule
        self.token_side = token_side


def _round_optional(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 8)


def _compute_sigma_series(cex_closes: list[float], rolling_window: int) -> dict[int, float | None]:
    returns_window: deque[float] = deque(maxlen=rolling_window)
    out: dict[int, float | None] = {}
    previous_close: float | None = None

    for idx, close in enumerate(cex_closes):
        if previous_close is not None and previous_close > 0.0 and close > 0.0:
            import math

            returns_window.append(math.log(close / previous_close))

        sigma = None
        if len(returns_window) >= 2:
            mean = sum(returns_window) / len(returns_window)
            variance = (
                sum((item - mean) ** 2 for item in returns_window)
                / (len(returns_window) - 1)
            )
            sigma = math.sqrt(max(variance, 0.0))
        out[idx] = sigma
        previous_close = close

    return out


def _compute_expected_steps(case: dict) -> list[dict]:
    cfg = case["config"]
    sigma_by_idx = _compute_sigma_series(case["cex_closes"], cfg["rolling_window_minutes"])
    rule = report.MarketRule(**case["market_rule"])
    histories = {
        "yes": deque(maxlen=cfg["rolling_window_minutes"]),
        "no": deque(maxlen=cfg["rolling_window_minutes"]),
    }
    active_token_side: str | None = None
    out: list[dict] = []

    for idx, row in enumerate(case["poly_observations"]):
        ts_ms = int(row["ts_ms"])
        cex_reference_price = float(case["cex_closes"][idx])
        raw_sigma = sigma_by_idx[idx]
        sigma = report.sigma_for_live_decision(raw_sigma, idx)
        minutes_to_expiry = max((case["settlement_ts_ms"] - ts_ms) / 60_000.0, 0.0)

        snapshots: dict[str, dict] = {}
        for token_side in ("yes", "no"):
            contract = _Contract(rule, token_side.upper())
            poly_price = float(row[f"{token_side}_price"])
            fair_value = report.token_fair_value(
                contract, cex_reference_price, sigma, minutes_to_expiry
            )
            delta = report.token_delta(contract, cex_reference_price, sigma, minutes_to_expiry)
            signal_basis = poly_price - fair_value
            z_score = report.rolling_zscore(histories[token_side], signal_basis)
            snapshots[token_side] = {
                "token_side": token_side,
                "poly_price": round(poly_price, 8),
                "fair_value": round(fair_value, 8),
                "signal_basis": round(signal_basis, 8),
                "z_score": _round_optional(z_score),
                "delta": round(delta, 8),
                "cex_reference_price": round(cex_reference_price, 8),
                "sigma": _round_optional(sigma),
                "minutes_to_expiry": round(minutes_to_expiry, 8),
            }

        candidate_sides = [
            token_side
            for token_side in ("yes", "no")
            if snapshots[token_side]["z_score"] is not None
        ]
        candidate_sides.sort(
            key=lambda token_side: (
                snapshots[token_side]["z_score"],
                -abs(snapshots[token_side]["signal_basis"]),
            )
        )

        preferred_side: str | None = active_token_side
        if preferred_side is None:
            ranked_sides = list(candidate_sides)
            if not ranked_sides:
                ranked_sides = ["yes", "no"]
                ranked_sides.sort(
                    key=lambda token_side: (
                        snapshots[token_side]["signal_basis"],
                        -abs(snapshots[token_side]["signal_basis"]),
                    )
                )
            preferred_side = ranked_sides[0] if ranked_sides else None
        signal: dict | None = None

        if active_token_side is not None:
            active_snapshot = snapshots[active_token_side]
            z_score = active_snapshot["z_score"]
            if z_score is not None and (
                abs(z_score) <= cfg["exit_z"] or z_score >= cfg["entry_z"]
            ):
                signal = {
                    "kind": "close",
                    "reason": "signal basis reverted inside exit band",
                }
                active_token_side = None
        else:
            for token_side in candidate_sides:
                snapshot = snapshots[token_side]
                z_score = snapshot["z_score"]
                if (
                    snapshot["minutes_to_expiry"] > 1.0
                    and z_score is not None
                    and z_score <= -cfg["entry_z"]
                    and snapshot["poly_price"] > 0.0
                    and snapshot["cex_reference_price"] > 0.0
                ):
                    signal = {
                        "kind": "open",
                        "token_side": token_side,
                        "poly_side": "buy",
                        "cex_side": "buy" if snapshot["delta"] < 0.0 else "sell",
                        "signal_basis": snapshot["signal_basis"],
                        "z_score": snapshot["z_score"],
                        "delta": snapshot["delta"],
                    }
                    active_token_side = token_side
                    preferred_side = token_side
                    break

        out.append(
            {
                "ts_ms": ts_ms,
                "preferred_snapshot": (
                    None if preferred_side is None else snapshots[preferred_side]
                ),
                "signal": signal,
            }
        )

        for token_side in ("yes", "no"):
            histories[token_side].append(snapshots[token_side]["signal_basis"])

    return out


class RustParityFixtureTest(unittest.TestCase):
    def test_fixture_matches_python_causal_reference(self) -> None:
        payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        self.assertTrue(payload["cases"], "fixture should contain at least one case")

        for case in payload["cases"]:
            with self.subTest(case=case["name"]):
                self.assertEqual(_compute_expected_steps(case), case["expected_steps"])


if __name__ == "__main__":
    unittest.main()
