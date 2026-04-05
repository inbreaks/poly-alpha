# Stable Backtest Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run an overnight multi-asset backtest sweep optimized for stable profitability, save raw artifacts, and generate a durable summary with per-asset conservative/balanced/aggressive recommendations.

**Architecture:** Add one reusable sweep runner under `scripts/` that owns the approved parameter matrix, phase orchestration, and report summarization. Keep the ranking logic explicit and testable so overnight runs and later reruns use the same stability-first scoring.

**Tech Stack:** Python 3, existing `target/debug/polyalpha-cli backtest rust-replay`, `unittest`, JSON/CSV/Markdown artifacts.

---

### Task 1: Add Sweep Plan Tests

**Files:**
- Create: `scripts/tests/test_stable_backtest_sweep.py`
- Test: `scripts/tests/test_stable_backtest_sweep.py`

- [ ] **Step 1: Write the failing test**

```python
from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


sweep = load_script_module(
    "polyalpha_stable_backtest_sweep",
    "scripts/stable_backtest_sweep.py",
)


class StableBacktestSweepTests(unittest.TestCase):
    def test_phase1_matrix_includes_default_band_and_wider_max_range(self) -> None:
        combos = sweep.build_phase1_runs()
        btc = [item for item in combos if item.asset == "btc"]
        self.assertTrue(any(item.min_poly_price == 0.20 and item.max_poly_price == 0.50 for item in btc))
        self.assertTrue(any(item.max_poly_price == 0.65 for item in btc))

    def test_select_top_phase1_candidates_prefers_stability_and_trade_count_floor(self) -> None:
        rows = [
            sweep.SweepRow("btc", 0.10, 0.50, None, None, 1000.0, 100.0, 0.60, 80, "phase1/a.json"),
            sweep.SweepRow("btc", 0.14, 0.50, None, None, 900.0, 60.0, 0.62, 75, "phase1/b.json"),
            sweep.SweepRow("btc", 0.20, 0.50, None, None, 1100.0, 100.0, 0.59, 30, "phase1/c.json"),
        ]
        selected = sweep.select_top_phase1_candidates(rows, top_n=2, min_round_trip_ratio=0.7)
        self.assertEqual([(item.min_poly_price, item.max_poly_price) for item in selected], [(0.14, 0.50), (0.10, 0.50)])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`
Expected: FAIL with module/file not found for `scripts/stable_backtest_sweep.py`

- [ ] **Step 3: Write minimal implementation**

Create `scripts/stable_backtest_sweep.py` with:

```python
def build_phase1_runs() -> list[SweepRun]:
    ...


def select_top_phase1_candidates(rows: list[SweepRow], top_n: int, min_round_trip_ratio: float) -> list[SweepRow]:
    ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/tests/test_stable_backtest_sweep.py scripts/stable_backtest_sweep.py
git commit -m "feat: add stable backtest sweep runner"
```

### Task 2: Implement Sweep Runner and Artifact Layout

**Files:**
- Modify: `scripts/stable_backtest_sweep.py`
- Create: `data/reports/stable_sweep/.gitkeep`

- [ ] **Step 1: Extend the runner to generate all three phases**

Add:

```python
def build_phase2_runs(phase1_rows: list[SweepRow]) -> list[SweepRun]:
    ...


def build_phase3_runs(phase2_rows: list[SweepRow]) -> list[SweepRun]:
    ...
```

- [ ] **Step 2: Add CLI entrypoints for `run`, `summarize`, and `plan`**

Support:

```bash
python3 scripts/stable_backtest_sweep.py plan
python3 scripts/stable_backtest_sweep.py run --output-dir data/reports/stable_sweep/<run_id> --max-workers 2
python3 scripts/stable_backtest_sweep.py summarize --output-dir data/reports/stable_sweep/<run_id>
```

- [ ] **Step 3: Persist raw and derived artifacts**

Write:

```text
data/reports/stable_sweep/<run_id>/
  manifest.json
  phase1/*.json
  phase2/*.json
  phase3/*.json
  summary.csv
  summary.json
  summary.md
```

- [ ] **Step 4: Run targeted test suite**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/stable_backtest_sweep.py data/reports/stable_sweep/.gitkeep
git commit -m "feat: add overnight stable sweep orchestration"
```

### Task 3: Execute Overnight Sweep and Save Summary

**Files:**
- Write: `data/reports/stable_sweep/<run_id>/*`
- Create: `docs/current-status.md` or dedicated summary note only if needed after results are in

- [ ] **Step 1: Dry-run the plan output**

Run: `python3 scripts/stable_backtest_sweep.py plan`
Expected: prints phase counts, parameter grid, and ranking rules

- [ ] **Step 2: Start the overnight run**

Run: `python3 scripts/stable_backtest_sweep.py run --output-dir data/reports/stable_sweep/<run_id> --max-workers 2`
Expected: phase progress logs plus report JSON artifacts under the output directory

- [ ] **Step 3: Generate saved summary**

Run: `python3 scripts/stable_backtest_sweep.py summarize --output-dir data/reports/stable_sweep/<run_id>`
Expected: `summary.csv`, `summary.json`, and `summary.md`

- [ ] **Step 4: Verify artifact completeness**

Run: `find data/reports/stable_sweep/<run_id> -maxdepth 2 -type f | sort`
Expected: phase outputs plus summary files are present

- [ ] **Step 5: Commit**

```bash
git add data/reports/stable_sweep/<run_id> docs/current-status.md
git commit -m "docs: save overnight stable sweep results"
```
