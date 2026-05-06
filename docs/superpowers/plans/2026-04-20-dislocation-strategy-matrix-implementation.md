# Pulse Dislocation Strategy Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal 15m dislocation confirmation matrix to the Python research chain so we can test whether post-pulse confirmation features improve 15m profitable exits across the two existing outcome datasets.

**Architecture:** Keep the change inside the existing `scripts/pulse_research_sweep.py` workflow. Add a small set of research-only confirmation fields plus a hard-coded 15m strategy matrix evaluator that consumes enriched outcome frames and emits one checkpoint JSON summary. Avoid generic strategy DSLs or new runtime abstractions.

**Tech Stack:** Python 3, `pandas`, `unittest`, existing `pulse_research_sweep.py` CLI/checkpoint helpers

---

### Task 1: Add failing tests for confirmation feature helpers

**Files:**
- Modify: `scripts/tests/test_pulse_research_sweep.py`
- Modify: `scripts/pulse_research_sweep.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_compute_post_pulse_confirmation_metrics_uses_low_anchor_and_windows(self) -> None:
        samples = [
            sweep.ConfirmationSample(ts_ms=1_000, price=0.40, spread_bps=200.0, top3_bid_notional=100.0),
            sweep.ConfirmationSample(ts_ms=2_000, price=0.30, spread_bps=500.0, top3_bid_notional=80.0),
            sweep.ConfirmationSample(ts_ms=3_000, price=0.34, spread_bps=430.0, top3_bid_notional=90.0),
            sweep.ConfirmationSample(ts_ms=5_000, price=0.37, spread_bps=410.0, top3_bid_notional=120.0),
        ]

        metrics = sweep.compute_post_pulse_confirmation_metrics(
            samples=samples,
            window_high_price=0.40,
            window_low_price=0.30,
            window_low_ts_ms=2_000,
            now_ts_ms=5_000,
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertTrue(metrics.confirm_window_complete_1s)
        self.assertTrue(metrics.confirm_window_complete_3s)
        self.assertAlmostEqual(metrics.post_pulse_refill_ratio_1s, 0.4)
        self.assertAlmostEqual(metrics.post_pulse_refill_ratio_3s, 0.7)
        self.assertAlmostEqual(metrics.spread_snapback_bps, 70.0)
        self.assertAlmostEqual(metrics.bid_depth_rebuild_ratio, 1.5)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_compute_post_pulse_confirmation_metrics_uses_low_anchor_and_windows`

Expected: FAIL with `AttributeError` or `NameError` because `ConfirmationSample` / `compute_post_pulse_confirmation_metrics` do not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
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
```

```python
def compute_post_pulse_confirmation_metrics(
    *,
    samples: list[ConfirmationSample],
    window_high_price: float,
    window_low_price: float,
    window_low_ts_ms: int,
    now_ts_ms: int,
) -> ConfirmationMetrics | None:
    ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_compute_post_pulse_confirmation_metrics_uses_low_anchor_and_windows`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/pulse_research_sweep.py scripts/tests/test_pulse_research_sweep.py
git commit -m "test: add post-pulse confirmation metric coverage"
```

### Task 2: Add failing tests for incomplete confirmation windows and matrix ranking

**Files:**
- Modify: `scripts/tests/test_pulse_research_sweep.py`
- Modify: `scripts/pulse_research_sweep.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_compute_post_pulse_confirmation_metrics_marks_incomplete_windows(self) -> None:
        samples = [
            sweep.ConfirmationSample(ts_ms=1_000, price=0.40, spread_bps=200.0, top3_bid_notional=100.0),
            sweep.ConfirmationSample(ts_ms=2_000, price=0.30, spread_bps=500.0, top3_bid_notional=80.0),
            sweep.ConfirmationSample(ts_ms=2_500, price=0.31, spread_bps=480.0, top3_bid_notional=82.0),
        ]

        metrics = sweep.compute_post_pulse_confirmation_metrics(
            samples=samples,
            window_high_price=0.40,
            window_low_price=0.30,
            window_low_ts_ms=2_000,
            now_ts_ms=2_500,
        )

        assert metrics is not None
        self.assertFalse(metrics.confirm_window_complete_1s)
        self.assertFalse(metrics.confirm_window_complete_3s)
        self.assertIsNone(metrics.post_pulse_refill_ratio_1s)
        self.assertIsNone(metrics.post_pulse_refill_ratio_3s)
        self.assertIsNone(metrics.spread_snapback_bps)
        self.assertIsNone(metrics.bid_depth_rebuild_ratio)
```

```python
    def test_rank_dislocation_strategy_matrix_filters_by_positive_days_and_delta(self) -> None:
        day1 = sweep.pd.DataFrame(
            [
                {
                    "symbol": "btc-a",
                    "claim_side": "no",
                    "ts_ms": 10_000,
                    "asset": "btc",
                    "entry_price": 0.10,
                    "min_tick_size": 0.01,
                    "fair_claim_price": 0.20,
                    "window_high_price": 0.18,
                    "current_mid": 0.11,
                    "filled_notional_usd": 100.0,
                    "current_bid": 0.09,
                    "current_ask": 0.10,
                    "window_displacement_bps": 1800.0,
                    "flow_refill_ratio": 0.20,
                    "fair_claim_move_bps": 2.0,
                    "cex_mid_move_bps": 2.0,
                    "flow_extreme_age_ms": 500,
                    "residual_basis_bps": 500.0,
                    "anchor_follow_ratio": 0.10,
                    "market_rule_kind": "above",
                    "time_to_settlement_hours": 48.0,
                    "post_pulse_refill_ratio_1s": 0.30,
                    "post_pulse_refill_ratio_3s": 0.40,
                    "spread_snapback_bps": 80.0,
                    "bid_depth_rebuild_ratio": 1.50,
                    "confirm_window_complete_1s": True,
                    "confirm_window_complete_3s": True,
                    "outcome_future_max_touch_15m": 0.11,
                    "outcome_deadline_bid_15m": 0.09,
                },
            ]
        )
        day2 = day1.copy()
        ranked = sweep.rank_dislocation_strategy_matrix(
            frames_by_label={"day1": day1, "day2": day2},
            poly_fee_bps=0.0,
            cex_taker_fee_bps=0.0,
            cex_slippage_bps=0.0,
            exit_config=sweep.AggressiveExitConfig("15m", 1, "any_touch", "raw_tick"),
            min_trades_per_day=1,
        )
        self.assertEqual(ranked[0]["strategy_name"], "structure_core")
        self.assertIn("delta_vs_structure_core", ranked[0])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_compute_post_pulse_confirmation_metrics_marks_incomplete_windows scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_rank_dislocation_strategy_matrix_filters_by_positive_days_and_delta`

Expected: FAIL because the incomplete-window behavior and matrix ranking helper do not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
def build_dislocation_strategy_specs() -> list[dict[str, Any]]:
    return [
        {"strategy_name": "structure_core", "filters": ["setup=directional_dislocation", "disp>=1500", "refill<=0.35"]},
        ...
    ]
```

```python
def rank_dislocation_strategy_matrix(
    *,
    frames_by_label: dict[str, pd.DataFrame],
    poly_fee_bps: float,
    cex_taker_fee_bps: float,
    cex_slippage_bps: float,
    exit_config: AggressiveExitConfig,
    min_trades_per_day: int = 1,
) -> list[dict[str, Any]]:
    ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_compute_post_pulse_confirmation_metrics_marks_incomplete_windows scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_rank_dislocation_strategy_matrix_filters_by_positive_days_and_delta`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/pulse_research_sweep.py scripts/tests/test_pulse_research_sweep.py
git commit -m "test: cover incomplete confirmation windows and strategy matrix"
```

### Task 3: Wire confirmation fields into feature rows and add a minimal CLI runner

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Modify: `scripts/tests/test_pulse_research_sweep.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_build_research_feature_row_includes_confirmation_fields(self) -> None:
        ...
        self.assertTrue(feature_row.confirm_window_complete_1s)
        self.assertTrue(feature_row.confirm_window_complete_3s)
        self.assertGreater(feature_row.post_pulse_refill_ratio_1s, 0.0)
        self.assertGreater(feature_row.post_pulse_refill_ratio_3s, 0.0)
```

```python
    def test_run_dislocation_strategy_matrix_writes_checkpoint(self) -> None:
        ...
        self.assertEqual(payload["exit_config_name"], "15m:any_touch:raw_tick:ticks1")
        self.assertTrue(payload["ranked_strategies"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_build_research_feature_row_includes_confirmation_fields scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_run_dislocation_strategy_matrix_writes_checkpoint`

Expected: FAIL because feature rows do not carry the new fields and the CLI/checkpoint entry point does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
@dataclass(frozen=True)
class ResearchFeatureRow:
    ...
    post_pulse_refill_ratio_1s: float | None = None
    post_pulse_refill_ratio_3s: float | None = None
    spread_snapback_bps: float | None = None
    bid_depth_rebuild_ratio: float | None = None
    confirm_window_complete_1s: bool = False
    confirm_window_complete_3s: bool = False
```

```python
def run_dislocation_strategy_matrix(...):
    ...
    return write_checkpoint(output_dir, "dislocation_strategy_matrix", label_prefix, payload)
```

```python
parser.add_argument("--matrix-outcome", action="append", default=None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_build_research_feature_row_includes_confirmation_fields scripts.tests.test_pulse_research_sweep.PulseResearchSweepTests.test_run_dislocation_strategy_matrix_writes_checkpoint`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/pulse_research_sweep.py scripts/tests/test_pulse_research_sweep.py
git commit -m "feat: add 15m dislocation strategy matrix runner"
```

### Task 4: Run the full verification and the two-day matrix

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Modify: `scripts/tests/test_pulse_research_sweep.py`
- Modify: `data/reports/pulse_research/`

- [ ] **Step 1: Run the target unit suite**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep`

Expected: PASS with all tests green.

- [ ] **Step 2: Run the dislocation matrix on the approved two-day sample**

Run:

```bash
PYTHONPATH=/Users/le/Documents/source-code/poly-alpha .venv/bin/python scripts/pulse_research_sweep.py \
  --stage dislocation-strategy-matrix \
  --output-label dislocation_matrix_15m \
  --matrix-outcome 20260418=/Users/le/Documents/source-code/poly-alpha/data/reports/pulse_research/features/pulse-aggressive-outcomes-overnight_aggressive-20260418T165101Z.jsonl.zst \
  --matrix-outcome 20260420_partial=/Users/le/Documents/source-code/poly-alpha/data/reports/pulse_research/features/pulse-aggressive-outcomes-20260420_partial_snapshot_clean-20260420T041401Z.jsonl.zst
```

Expected: Prints the checkpoint JSON path.

- [ ] **Step 3: Inspect the checkpoint payload**

Run:

```bash
latest=$(ls -t /Users/le/Documents/source-code/poly-alpha/data/reports/pulse_research/checkpoints/dislocation_strategy_matrix-dislocation_matrix_15m-*.json | head -n 1)
sed -n '1,240p' "$latest"
```

Expected: Includes `exit_config_name`, `ranked_strategies`, and per-day stats.

- [ ] **Step 4: Commit**

```bash
git add scripts/pulse_research_sweep.py scripts/tests/test_pulse_research_sweep.py docs/superpowers/specs/2026-04-20-dislocation-strategy-matrix-design.md docs/superpowers/plans/2026-04-20-dislocation-strategy-matrix-implementation.md
git commit -m "feat: add 15m dislocation confirmation matrix research"
```
