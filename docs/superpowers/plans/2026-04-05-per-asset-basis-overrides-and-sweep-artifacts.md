# Per-Asset Basis Overrides And Sweep Artifacts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Save the stable-sweep conclusions and recommended config, implement complete per-asset basis overrides, and add per-asset equity/drawdown chart artifacts for the chosen benchmark combinations.

**Architecture:** Centralize basis override resolution into one effective-config path, then reuse it in live runtime, monitor serialization, and artifact generation. Keep sweep artifact generation script-driven so reports, recommended config, and charts are reproducible from code rather than manual notes.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-cli`, `polyalpha-engine`), Python stdlib scripts, existing `rust-replay` CSV outputs, `matplotlib`, inline Rust unit tests, Python `unittest`.

---

### Task 1: Add Failing Tests For Full Basis Override Resolution

**Files:**
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-core/src/config.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Write failing config-resolution tests**

Add tests that deserialize:

```json
{
  "entry_z_score_threshold": "4.0",
  "exit_z_score_threshold": "0.5",
  "rolling_window_secs": 36000,
  "min_warmup_samples": 600,
  "min_basis_bps": "50.0",
  "max_position_usd": "200",
  "max_open_instant_loss_pct_of_budget": "0.01",
  "delta_rebalance_threshold": "0.05",
  "delta_rebalance_interval_secs": 60,
  "band_policy": "configured_band",
  "min_poly_price": "0.2",
  "max_poly_price": "0.5",
  "max_data_age_minutes": 1,
  "max_time_diff_minutes": 1,
  "enable_freshness_check": true,
  "reject_on_disconnect": true,
  "overrides": {
    "btc": {
      "min_poly_price": "0.10",
      "max_poly_price": "0.45",
      "max_open_instant_loss_pct_of_budget": "0.02",
      "enable_freshness_check": false
    }
  }
}
```

And assert the effective BTC config differs from the global values while ETH keeps the defaults.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p polyalpha-core basis_strategy_config_defaults_open_instant_loss_pct per_asset_basis_override_resolution -- --nocapture`

Expected: FAIL because full override resolution helpers and fields are missing.

- [ ] **Step 3: Write failing live-paper tests**

Add a paper-layer test that builds settings with a BTC override and verifies:
- BTC price filter uses `0.10-0.45`
- ETH price filter still uses global band
- Monitor config snapshot exposes per-asset overrides

- [ ] **Step 4: Run targeted paper tests to verify they fail**

Run: `cargo test -p polyalpha-cli per_asset_basis -- --nocapture`

Expected: FAIL because paper/monitor still read global-only fields.

### Task 2: Implement Full Effective Basis Config Path

**Files:**
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-cli/src/runtime.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`

- [ ] **Step 1: Extend `BasisOverrideConfig` with all basis fields that should support asset overrides**

Include:
- `max_open_instant_loss_pct_of_budget`
- `delta_rebalance_threshold`
- `delta_rebalance_interval_secs`
- `max_data_age_minutes`
- `max_time_diff_minutes`
- `enable_freshness_check`
- `reject_on_disconnect`

- [ ] **Step 2: Add effective-config helpers**

Add a reusable resolved type and helpers along the lines of:

```rust
pub struct EffectiveBasisConfig { ... }

impl BasisStrategyConfig {
    pub fn effective_for_asset_key(&self, asset_key: &str) -> EffectiveBasisConfig { ... }
}
```

- [ ] **Step 3: Replace paper/live global reads with effective per-asset reads**

Update call sites that currently use `settings.strategy.basis.*` directly for market-specific behavior:
- signal price filter
- per-market freshness rules
- delta rebalance threshold/interval
- planner instant-open-loss tolerance
- engine market overrides

- [ ] **Step 4: Expose override-aware config in monitor payload**

Add per-asset effective config summary to `MonitorStrategyConfig` so the UI can show that BTC/ETH/SOL/XRP are running distinct values.

- [ ] **Step 5: Run focused Rust tests**

Run:
- `cargo test -p polyalpha-core per_asset_basis -- --nocapture`
- `cargo test -p polyalpha-cli per_asset_basis -- --nocapture`

Expected: PASS

### Task 3: Save Stable Sweep Report And Recommended Config

**Files:**
- Create: `docs/stable-sweep-2026-04-05.md`
- Create: `config/multi-market-active.stable.toml`
- Modify: `scripts/stable_backtest_sweep.py`
- Test: `scripts/tests/test_stable_backtest_sweep.py`

- [ ] **Step 1: Write failing script tests for report/config generation**

Add Python tests that assert the sweep script can emit:
- a Markdown report path
- a recommended TOML config path
- report sections covering methodology, selected defaults, and concentration notes

- [ ] **Step 2: Run Python tests to verify they fail**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`

Expected: FAIL because report/config emitters do not exist yet.

- [ ] **Step 3: Implement report/config emitters**

Generate:
- `docs/stable-sweep-2026-04-05.md`
- `config/multi-market-active.stable.toml`

The recommended config must encode the chosen per-asset overrides:
- BTC: `min=0.10`, `max=0.45`
- ETH: `min=0.14`, `max=0.65`, `entry_z=4.0`, `rolling_window_secs=21600`, `exit_z=0.6`
- SOL: `min=0.24`, `max=0.65`, `entry_z=4.0`, `rolling_window_secs=36000`, `exit_z=0.6`
- XRP: `min=0.06`, `max=0.65`

- [ ] **Step 4: Re-run Python tests**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`

Expected: PASS

### Task 4: Export Benchmark Equity/Drawdown Artifacts

**Files:**
- Modify: `scripts/stable_backtest_sweep.py`
- Create: `data/reports/stable_sweep/2026-04-05-overnight-stability/charts/*`
- Test: `scripts/tests/test_stable_backtest_sweep.py`

- [ ] **Step 1: Write failing chart-generation tests**

Add tests that expect helper functions to:
- rerun selected combos with `--equity-csv`
- build drawdown series from equity CSV
- emit per-asset `png` and `svg`

- [ ] **Step 2: Run Python tests to verify they fail**

Run: `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`

Expected: FAIL because chart/export helpers do not exist.

- [ ] **Step 3: Implement chart export for benchmark combos**

Per asset, include three lines:
- current default
- chosen stable default
- chosen aggressive/default-high-return reference

Write:
- `charts/<asset>-equity.png`
- `charts/<asset>-equity.svg`
- `charts/<asset>-drawdown.png`
- `charts/<asset>-drawdown.svg`

- [ ] **Step 4: Generate artifacts for the saved overnight run**

Run: `python3 scripts/stable_backtest_sweep.py export-benchmark-charts --output-dir data/reports/stable_sweep/2026-04-05-overnight-stability`

Expected: chart files plus benchmark equity CSVs under the run directory.

### Task 5: Full Verification And Acceptance

**Files:**
- Verify: `docs/stable-sweep-2026-04-05.md`
- Verify: `config/multi-market-active.stable.toml`
- Verify: `data/reports/stable_sweep/2026-04-05-overnight-stability/*`

- [ ] **Step 1: Run complete targeted verification**

Run:
- `python3 -m unittest scripts.tests.test_stable_backtest_sweep -v`
- `cargo test -p polyalpha-core per_asset_basis -- --nocapture`
- `cargo test -p polyalpha-cli per_asset_basis -- --nocapture`
- `cargo build -p polyalpha-cli`

- [ ] **Step 2: Verify artifact presence**

Run:

```bash
find docs -maxdepth 1 -name 'stable-sweep-2026-04-05.md'
find config -maxdepth 1 -name 'multi-market-active.stable.toml'
find data/reports/stable_sweep/2026-04-05-overnight-stability -maxdepth 2 -type f | sort
```

Expected: report, config, summary files, and chart files are present.

- [ ] **Step 3: Manual acceptance check**

Confirm:
- report records the recommendation and concentration analysis
- config can be loaded as a normal env file
- per-asset overrides are visible in monitor payload / UI summary
- chart outputs show both equity and drawdown per asset
