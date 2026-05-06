# Geometry Headroom Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the geometry signal matrix so it filters out near-boundary terminal tickets first, then splits short-horizon signals into short rebound versus deeper reversion rows.

**Architecture:** Keep the existing research script structure intact. Add one derived geometry field for boundary headroom, replace the current rebuild-centric strategy masks with a smaller headroom-plus-mode matrix, and adjust the tests so the new matrix behavior is explicit.

**Tech Stack:** Python, pandas, unittest, existing `scripts/pulse_research_sweep.py` research CLI

---

### Task 1: Lock the new matrix behavior in tests

**Files:**
- Modify: `scripts/tests/test_pulse_research_sweep.py`
- Test: `scripts/tests/test_pulse_research_sweep.py`

- [ ] **Step 1: Write the failing test for the new strategy names and headroom field**

```python
def test_build_geometry_signal_strategy_masks_uses_boundary_headroom_and_mode_split(self) -> None:
    frame = sweep.pd.DataFrame([...])
    masks = sweep.build_geometry_signal_strategy_masks(frame)
    names = [name for name, _, _ in masks]
    self.assertEqual(
        names,
        [
            "geom_headroom10_core",
            "geom_headroom20_core",
            "geom_elastic_snapback",
            "geom_deep_reversion",
            "geom_terminal_zone_control",
        ],
    )
    derived = sweep.derive_dislocation_reachability_frame(...)
    self.assertIn("boundary_headroom_ticks", derived.columns)
```

- [ ] **Step 2: Run the focused test and verify it fails for the expected reason**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.TestPulseResearchSweep.test_build_geometry_signal_strategy_masks_uses_boundary_headroom_and_mode_split`

Expected: FAIL because the derived frame does not yet expose `boundary_headroom_ticks` and the strategy names still use the old `geom_rebuild_*` matrix.

- [ ] **Step 3: Update the matrix evaluation test to assert the new counts**

```python
base = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_headroom10_core")
snapback = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_elastic_snapback")
deep = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_deep_reversion")
terminal = next(item for item in result["strategy_stats"] if item["strategy_name"] == "geom_terminal_zone_control")
self.assertEqual(base["day_stats"]["day1"]["trade_count"], 2)
self.assertEqual(snapback["day_stats"]["day1"]["trade_count"], 1)
self.assertEqual(deep["day_stats"]["day1"]["trade_count"], 1)
self.assertEqual(terminal["day_stats"]["day1"]["trade_count"], 1)
```

- [ ] **Step 4: Run the focused evaluation test and verify it fails**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.TestPulseResearchSweep.test_evaluate_geometry_signal_matrix_uses_symbol_filter_and_strategy_masks`

Expected: FAIL because the production matrix still emits the old strategy names and old rebuild-based filters.

### Task 2: Implement the boundary-headroom geometry matrix

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Test: `scripts/tests/test_pulse_research_sweep.py`

- [ ] **Step 1: Add the derived boundary headroom field in the reachability frame**

```python
boundary_headroom_ticks = pd.concat(
    [
        (1.0 - entry_price) / tick_size.where(tick_size > 0.0, float("nan")),
        entry_price / tick_size.where(tick_size > 0.0, float("nan")),
    ],
    axis=1,
).min(axis=1)

derived["boundary_headroom_ticks"] = boundary_headroom_ticks
```

- [ ] **Step 2: Replace the rebuild-centric matrix with the new rows**

```python
tradable_geom = universe & (boundary_headroom_ticks > 10.0)
strong_geom = universe & (boundary_headroom_ticks > 20.0)
shock_core = tradable_geom & (window_displacement >= 40.0)

return [
    ("geom_headroom10_core", [...], shock_core),
    ("geom_headroom20_core", [...], strong_geom & (window_displacement >= 40.0)),
    ("geom_elastic_snapback", [...], strong_geom & (window_displacement >= 40.0) & (levels_consumed >= 2) & (anchor_follow > 0.60)),
    ("geom_deep_reversion", [...], strong_geom & (window_displacement >= 40.0) & (levels_consumed >= 2) & (anchor_follow <= 0.15)),
    ("geom_terminal_zone_control", [...], universe & (boundary_headroom_ticks <= 5.0)),
]
```

- [ ] **Step 3: Extend matrix day stats with the new headroom median**

```python
"boundary_headroom_ticks_p50": _median_or_none(
    pd.to_numeric(
        _frame_column(subset, "boundary_headroom_ticks", float("nan")),
        errors="coerce",
    ).dropna()
),
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep.TestPulseResearchSweep.test_build_geometry_signal_strategy_masks_uses_boundary_headroom_and_mode_split scripts.tests.test_pulse_research_sweep.TestPulseResearchSweep.test_evaluate_geometry_signal_matrix_uses_symbol_filter_and_strategy_masks scripts.tests.test_pulse_research_sweep.TestPulseResearchSweep.test_run_geometry_signal_matrix_writes_checkpoint`

Expected: PASS

- [ ] **Step 5: Run the full unit test file**

Run: `.venv/bin/python -m unittest scripts.tests.test_pulse_research_sweep`

Expected: PASS

### Task 3: Run one verification matrix with the new rules

**Files:**
- Modify: `scripts/pulse_research_sweep.py`
- Test: `data/reports/pulse_research/dislocation_matrix_20260420/...`

- [ ] **Step 1: Re-run the geometry signal matrix on the existing outcome files**

Run:

```bash
.venv/bin/python scripts/pulse_research_sweep.py \
  --config config/multi-market-active.fresh.toml \
  --output-dir data/reports/pulse_research/dislocation_matrix_20260420 \
  --stage geometry-signal-matrix \
  --matrix-outcome '20260418=data/reports/pulse_research/dislocation_matrix_20260420/features/pulse-aggressive-outcomes-dislocation_20260418_15m-20260420T072522Z.jsonl.zst' \
  --matrix-outcome '20260420_partial=data/reports/pulse_research/dislocation_matrix_20260420/features/pulse-aggressive-outcomes-dislocation_20260420_partial_15m_retry-20260420T080443Z.jsonl.zst' \
  --matrix-min-trades-per-day 1 \
  --output-label geometry_signal_headroom_15m
```

Expected: checkpoint and summary are written under `data/reports/pulse_research/dislocation_matrix_20260420/`.

- [ ] **Step 2: Inspect the resulting summary for strategy counts and day-level separation**

Run: `sed -n '1,220p' data/reports/pulse_research/dislocation_matrix_20260420/geometry_signal_headroom_15m_summary_*.md`

Expected: output shows the new strategy names and whether headroom-filtered rows separate from `geom_terminal_zone_control`.
