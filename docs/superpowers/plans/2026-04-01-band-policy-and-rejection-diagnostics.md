# Band Policy and Rejection Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Polymarket price-band filtering explicit, machine-readable, and comparable across backtest, paper/sim, and monitor outputs so we can distinguish true missing-price failures from configured band-policy rejections and cleanly compare `current`, `current-noband`, and `legacy`.

**Architecture:** First extend Rust replay diagnostics so every rejected opening signal is attributed to an explicit reason code and every market gets a reason-count breakdown. Then thread an explicit `band_policy` through config, CLI, runtime, and socket transport while keeping the live default conservative (`configured_band`). Finally add first-class three-way reporting (`current`, `current-noband`, `legacy`) so analysis no longer depends on one-off ad hoc scripts.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-cli`), `serde`, `clap`, existing inline Rust unit tests, Python stdlib report scripts, `duckdb`, and SVG generation without extra plotting dependencies.

---

## File Structure

- Modify: `crates/polyalpha-cli/src/backtest_rust.rs`
  Responsibility: Add explicit replay rejection reason taxonomy, market-level rejection counters, serialized report fields, and regression tests for band-policy behavior.
- Modify: `crates/polyalpha-core/src/config.rs`
  Responsibility: Introduce explicit `band_policy` configuration for basis strategy defaults and overrides.
- Modify: `config/default.toml`
  Responsibility: Keep current live-safe behavior as the default policy while making the policy explicit in config.
- Modify: `crates/polyalpha-cli/src/args.rs`
  Responsibility: Add CLI support for explicit band policy selection in replay commands without breaking existing min/max overrides.
- Modify: `crates/polyalpha-core/src/socket/message.rs`
  Responsibility: Carry explicit band policy and machine-readable comparison/rejection fields over monitor transport and config updates.
- Modify: `crates/polyalpha-cli/src/runtime.rs`
  Responsibility: Plumb explicit band policy from settings into runtime defaults and preserve current live behavior.
- Modify: `crates/polyalpha-cli/src/paper.rs`
  Responsibility: Accept runtime band-policy updates, classify band-policy skips separately from generic missing-price skips, and keep rejection summaries machine-readable.
- Modify: `crates/polyalpha-cli/src/sim.rs`
  Responsibility: Preserve the same reason codes in simulation event output and keep the no-band replay path comparable to backtest.
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
  Responsibility: Store explicit band policy and reason counters in monitor state.
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
  Responsibility: Render explicit band policy and top rejection reasons instead of hiding them behind generic missing-price wording.
- Modify: `scripts/btc_basis_backtest_report.py`
  Responsibility: Add a first-class `run-current-noband` wrapper and expose explicit band-policy metadata in Chinese reports.
- Create: `scripts/backtest_compare_report.py`
  Responsibility: Produce `current/current-noband/legacy` summary CSV/JSON/SVG outputs from per-run artifacts.
- Modify: `scripts/tests/test_python_cli_report_layer.py`
  Responsibility: Cover new band-policy CLI/report plumbing from Python.
- Create: `scripts/tests/test_backtest_compare_report.py`
  Responsibility: Lock down three-way comparison output shape and SVG generation.

### Task 1: Separate Replay Rejection Reasons From Missing Price

**Files:**
- Modify: `crates/polyalpha-cli/src/backtest_rust.rs`
- Test: `crates/polyalpha-cli/src/backtest_rust.rs`

- [ ] **Step 1: Write failing replay diagnostics tests**

```rust
#[test]
fn separates_price_band_rejections_from_true_missing_quotes() {
    let spec = sample_replay_spec();
    let mut row = sample_replay_row(&spec);
    row.yes_price = Some(0.05);
    row.no_price = Some(0.95);
    let mut candidate = sample_open_candidate(&spec, &row);
    candidate.token_side = TokenSide::Yes;

    let mut config = sample_replay_config(1.0, 20.0, 50.0, 5.0, 2.0);
    config.min_poly_price = Some(Decimal::new(2, 1));
    config.max_poly_price = Some(Decimal::new(5, 1));

    let planner = replay_execution_planner(&config);
    let mut positions = HashMap::new();
    let mut cash = config.initial_capital;
    let mut stats = ReplayStats::default();
    let mut trade_log = Vec::new();

    handle_candidate(
        candidate,
        &spec,
        &row,
        100_000.0,
        &config,
        &planner,
        &mut positions,
        &mut cash,
        &mut stats,
        &mut trade_log,
    )
    .expect("candidate handling should not error");

    assert_eq!(stats.signals_rejected_missing_price, 0);
    assert_eq!(stats.signals_rejected_below_min_poly_price, 1);
    assert_eq!(stats.signals_rejected_above_or_equal_max_poly_price, 0);
}

#[test]
fn market_debug_rows_include_reason_breakdown() {
    let mut stats = ReplayStats::default();
    stats.signals_by_market.insert("mkt-1".to_owned(), 3);
    stats.rejected_below_min_price_by_market
        .insert("mkt-1".to_owned(), 2);

    let rows = all_market_debug_rows(&stats);
    assert_eq!(rows[0].signal_count, 3);
    assert_eq!(rows[0].rejected_below_min_price_count, 2);
    assert_eq!(rows[0].rejected_missing_price_count, 0);
}
```

- [ ] **Step 2: Run the targeted replay tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-cli separates_price_band_rejections_from_true_missing_quotes -- --nocapture
cargo test -p polyalpha-cli market_debug_rows_include_reason_breakdown -- --nocapture
```

Expected: compile or assertion failure because `ReplayStats` and `MarketSignalDebugRow` do not yet expose explicit band-rejection counters.

- [ ] **Step 3: Implement explicit replay rejection reason counters**

```rust
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum ReplaySignalRejectionReason {
    MissingQuote,
    BelowMinPolyPrice,
    AboveOrEqualMaxPolyPrice,
    PlannerRejected,
    CapitalRejected,
    ExistingPosition,
}

#[derive(Clone, Debug, Default)]
struct ReplayStats {
    // existing fields ...
    signals_rejected_missing_price: usize,
    signals_rejected_below_min_poly_price: usize,
    signals_rejected_above_or_equal_max_poly_price: usize,
    rejected_missing_price_by_market: HashMap<String, usize>,
    rejected_below_min_price_by_market: HashMap<String, usize>,
    rejected_above_or_equal_max_price_by_market: HashMap<String, usize>,
}

#[derive(Clone, Debug, Serialize)]
struct MarketSignalDebugRow {
    market_id: String,
    signal_count: usize,
    rejected_missing_price_count: usize,
    rejected_below_min_price_count: usize,
    rejected_above_or_equal_max_price_count: usize,
    rejected_capital_count: usize,
    entry_count: usize,
    net_pnl: f64,
}
```

Implementation notes:
- Replace the current `stats.signals_rejected_missing_price += 1` calls in the `min_poly_price` and `max_poly_price` branches with dedicated counters.
- Keep true `None` / `<= 0` quote failures in `signals_rejected_missing_price`.
- Serialize the new counters in `RustReplaySummary`.
- Keep old JSON fields until the Python layer is updated, but mark them as true missing-quote counts only.

- [ ] **Step 4: Re-run replay tests plus current regression coverage**

Run:

```bash
cargo test -p polyalpha-cli handle_candidate_rejects_invalid_hedge_qty -- --nocapture
cargo test -p polyalpha-cli separates_price_band_rejections_from_true_missing_quotes -- --nocapture
cargo test -p polyalpha-cli market_debug_rows_include_reason_breakdown -- --nocapture
```

Expected: all pass, and existing planner-rejection tests remain green.

- [ ] **Step 5: Commit the replay reason split**

```bash
git add crates/polyalpha-cli/src/backtest_rust.rs
git commit -m "feat: split replay price-band rejections from missing price"
```

### Task 2: Make Band Policy Explicit In Config, CLI, Runtime, And Socket Transport

**Files:**
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `config/default.toml`
- Modify: `crates/polyalpha-cli/src/args.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-cli/src/runtime.rs`
- Test: `crates/polyalpha-core/src/socket/message.rs`
- Test: `crates/polyalpha-cli/src/runtime.rs`

- [ ] **Step 1: Write failing config/transport tests**

```rust
#[test]
fn band_policy_roundtrips_through_config_update_json() {
    let msg = Message::Command {
        command_id: "cfg-1".to_owned(),
        kind: CommandKind::UpdateConfig {
            config: ConfigUpdate {
                band_policy: Some("disabled".to_owned()),
                min_poly_price: Some(0.0),
                max_poly_price: Some(1.0),
                ..ConfigUpdate::default()
            },
        },
    };

    let encoded = msg.to_bytes().unwrap();
    let decoded = Message::from_bytes(&encoded[..encoded.len() - 1]).unwrap();
    match decoded {
        Message::Command {
            kind: CommandKind::UpdateConfig { config },
            ..
        } => assert_eq!(config.band_policy.as_deref(), Some("disabled")),
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn sample_settings_keep_live_default_band_policy() {
    let settings = sample_settings();
    assert_eq!(
        settings.strategy.basis.band_policy,
        BandPolicyMode::ConfiguredBand
    );
}
```

- [ ] **Step 2: Run the targeted tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-core band_policy_roundtrips_through_config_update_json -- --nocapture
cargo test -p polyalpha-cli sample_settings_keep_live_default_band_policy -- --nocapture
```

Expected: failure because `band_policy` does not yet exist in config structs or config update messages.

- [ ] **Step 3: Add explicit `BandPolicyMode` plumbing**

```rust
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BandPolicyMode {
    #[default]
    ConfiguredBand,
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BasisStrategyConfig {
    // existing fields ...
    #[serde(default)]
    pub band_policy: BandPolicyMode,
    #[serde(default)]
    pub min_poly_price: Option<Decimal>,
    #[serde(default)]
    pub max_poly_price: Option<Decimal>,
}
```

Concrete plumbing:
- `config/default.toml`: add `band_policy = "configured_band"` under `[strategy.basis]`.
- `args.rs`: add `--band-policy <configured_band|disabled>` to `rust-replay`.
- `runtime.rs`: preserve live default by reading config policy and only using `Disabled` when explicitly configured.
- `socket/message.rs`: extend `ConfigUpdate` with `band_policy: Option<String>` and monitor-facing config snapshots with the resolved policy label.

- [ ] **Step 4: Re-run config/runtime tests**

Run:

```bash
cargo test -p polyalpha-core band_policy_roundtrips_through_config_update_json -- --nocapture
cargo test -p polyalpha-cli sample_settings_keep_live_default_band_policy -- --nocapture
cargo test -p polyalpha-cli build_execution_stack_uses_configured_execution_costs -- --nocapture
```

Expected: new tests pass, and runtime stack wiring still passes existing config tests.

- [ ] **Step 5: Commit explicit band policy plumbing**

```bash
git add config/default.toml crates/polyalpha-core/src/config.rs crates/polyalpha-core/src/socket/message.rs crates/polyalpha-cli/src/args.rs crates/polyalpha-cli/src/runtime.rs
git commit -m "feat: add explicit band policy configuration"
```

### Task 3: Add First-Class `current/current-noband/legacy` Reporting And Charts

**Files:**
- Modify: `scripts/btc_basis_backtest_report.py`
- Create: `scripts/backtest_compare_report.py`
- Modify: `scripts/tests/test_python_cli_report_layer.py`
- Create: `scripts/tests/test_backtest_compare_report.py`

- [ ] **Step 1: Write failing Python report tests**

```python
class PythonCliReportLayerTests(unittest.TestCase):
    def test_run_current_noband_command_disables_price_band(self) -> None:
        cfg = self.make_cfg()
        command = report.build_rust_replay_command(
            cfg,
            rust_report_json_path="/tmp/rust-report.json",
            rust_equity_csv_path="/tmp/rust-equity.csv",
            rust_anomaly_report_json_path="/tmp/rust-anomalies.json",
            band_policy="disabled",
        )
        self.assertIn("--band-policy", command)
        self.assertIn("disabled", command)


class BacktestCompareReportTests(unittest.TestCase):
    def test_compare_report_writes_three_way_summary(self) -> None:
        payload = compare.build_summary_payload(
            current_report={"summary": {"total_pnl": 10.0}},
            current_noband_report={"summary": {"total_pnl": 15.0}},
            legacy_report={"summary": {"total_pnl": 25.0}},
        )
        self.assertEqual(payload["rows"][0]["run_kind"], "current")
        self.assertEqual(payload["rows"][1]["run_kind"], "current_noband")
        self.assertEqual(payload["rows"][2]["run_kind"], "legacy")
```

- [ ] **Step 2: Run the Python tests and confirm they fail**

Run:

```bash
python3 -m unittest scripts.tests.test_python_cli_report_layer
python3 -m unittest scripts.tests.test_backtest_compare_report
```

Expected: failure because `build_rust_replay_command(..., band_policy=...)` and the new comparison module do not exist yet.

- [ ] **Step 3: Implement the report-layer changes**

```python
def build_rust_replay_command(
    cfg: RunConfig,
    rust_report_json_path: str,
    rust_equity_csv_path: str,
    rust_anomaly_report_json_path: str,
    *,
    band_policy: str | None = None,
) -> list[str]:
    args = [
        "cargo",
        "run",
        "-q",
        "-p",
        "polyalpha-cli",
        "--",
        "backtest",
        "rust-replay",
    ]
    if band_policy is not None:
        args.extend(["--band-policy", band_policy])
    return args
```

Implementation notes:
- In `btc_basis_backtest_report.py`, add `run-current-noband` as a thin wrapper over Rust replay with `band_policy="disabled"` and `min_poly_price/max_poly_price` left unset.
- Keep existing `run` semantics unchanged.
- In the new `scripts/backtest_compare_report.py`, consume per-run `report.json` + `equity.csv` artifacts and emit:
  - `summary.csv`
  - `summary.json`
  - aggregate SVG
  - per-market SVG
  - optional drilldown SVG for selected markets

- [ ] **Step 4: Re-run Python report tests and a real artifact smoke test**

Run:

```bash
python3 -m unittest scripts.tests.test_python_cli_report_layer
python3 -m unittest scripts.tests.test_backtest_compare_report
python3 scripts/backtest_compare_report.py --help
```

Expected: all tests pass, and the comparison script prints CLI help without import errors.

- [ ] **Step 5: Commit report tooling**

```bash
git add scripts/btc_basis_backtest_report.py scripts/backtest_compare_report.py scripts/tests/test_python_cli_report_layer.py scripts/tests/test_backtest_compare_report.py
git commit -m "feat: add three-way backtest comparison reporting"
```

### Task 4: Surface Band Policy And Rejection Reasons In Paper/Sim/Monitor

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-cli/src/sim.rs`
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-cli/src/sim.rs`

- [ ] **Step 1: Write failing runtime-facing diagnostics tests**

```rust
#[test]
fn apply_config_update_accepts_band_policy_disabled() {
    let mut settings = sample_settings();
    let update = ConfigUpdate {
        band_policy: Some("disabled".to_owned()),
        min_poly_price: Some(0.0),
        max_poly_price: Some(1.0),
        ..ConfigUpdate::default()
    };

    let changed = apply_config_update(&mut settings, &update).expect("config update");
    assert!(changed.iter().any(|item| item.contains("band policy=disabled")));
}

#[test]
fn sim_event_log_keeps_band_policy_rejection_code_visible() {
    let text = render_execution_event_text(&ExecutionEvent::OrderRejected {
        rejection_reason: Some("below_min_poly_price".to_owned()),
        ..sample_order_rejected_event()
    });
    assert!(text.contains("below_min_poly_price"));
}
```

- [ ] **Step 2: Run the targeted runtime tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-cli apply_config_update_accepts_band_policy_disabled -- --nocapture
cargo test -p polyalpha-cli sim_event_log_keeps_band_policy_rejection_code_visible -- --nocapture
```

Expected: failure because config updates and monitor text do not yet understand explicit band policy or split band reason codes.

- [ ] **Step 3: Implement runtime + monitor surfacing**

```rust
if let Some(band_policy) = update.band_policy.as_deref() {
    settings.strategy.basis.band_policy =
        parse_band_policy_mode(band_policy).context("invalid band policy")?;
    changed.push(format!("band policy={band_policy}"));
}

fn rejection_reason_label(reason: &str) -> &'static str {
    match reason {
        "below_min_poly_price" => "低于最小 Poly 价格带",
        "above_or_equal_max_poly_price" => "高于最大 Poly 价格带",
        "missing_poly_quote" => "缺少 Poly 报价",
        other => other,
    }
}
```

Implementation notes:
- In `paper.rs`, keep post-submit rejections and pre-submit skip reasons distinct.
- Add band-policy labels to `monitor/ui.rs` near the existing min/max price display.
- Do not change live execution behavior in this task; only improve explainability and transport.

- [ ] **Step 4: Re-run runtime diagnostics tests**

Run:

```bash
cargo test -p polyalpha-cli apply_config_update_accepts_band_policy_disabled -- --nocapture
cargo test -p polyalpha-cli sim_event_log_keeps_band_policy_rejection_code_visible -- --nocapture
cargo test -p polyalpha-cli track_open_signal_execution_outcome_pushes_chinese_rejection_summary -- --nocapture
```

Expected: new diagnostics tests pass and existing paper rejection-summary tests remain green.

- [ ] **Step 5: Commit runtime observability**

```bash
git add crates/polyalpha-cli/src/paper.rs crates/polyalpha-cli/src/sim.rs crates/polyalpha-cli/src/monitor/state.rs crates/polyalpha-cli/src/monitor/ui.rs
git commit -m "feat: expose band policy and rejection reasons in monitor"
```

### Task 5: Verify The End-To-End Diagnostic Loop

**Files:**
- Modify: `docs/superpowers/plans/2026-04-01-band-policy-and-rejection-diagnostics.md`
- Output: `tmp/multi_market/`
- Output: `tmp/market_drilldown/`

- [ ] **Step 1: Run focused replay verification for the three confirmed markets**

Run:

```bash
cargo run -q -p polyalpha-cli -- backtest rust-replay \
  --db-path data/eth_basis_backtest_2026.duckdb \
  --market-id 1266411 \
  --report-json tmp/market_drilldown/eth-1266411-current.report.json

cargo run -q -p polyalpha-cli -- backtest rust-replay \
  --db-path data/eth_basis_backtest_2026.duckdb \
  --market-id 1266411 \
  --band-policy disabled \
  --report-json tmp/market_drilldown/eth-1266411-current-noband.report.json
```

Expected: current report shows explicit `below_min_poly_price` / `above_or_equal_max_poly_price` counters; no-band report shifts those counts into planner or execution paths instead of generic missing-price.

- [ ] **Step 2: Rebuild three-way comparison artifacts**

Run:

```bash
python3 scripts/backtest_compare_report.py \
  --current-dir tmp/multi_market/current \
  --current-noband-dir tmp/multi_market/current_noband \
  --legacy-dir tmp/multi_market/legacy \
  --output-dir tmp/multi_market_compare
```

Expected: summary CSV/JSON and SVGs are emitted with all three run kinds.

- [ ] **Step 3: Verify monitor/runtime reason visibility**

Run:

```bash
cargo test -p polyalpha-cli build_monitor_events_keeps_rejection_visible_amid_system_noise -- --nocapture
cargo test -p polyalpha-cli track_open_signal_execution_outcome_pushes_chinese_rejection_summary -- --nocapture
```

Expected: band-policy-specific reason codes remain visible in user-facing monitor summaries.

- [ ] **Step 4: Run the full regression bundle for touched areas**

Run:

```bash
cargo test -p polyalpha-cli manager::tests:: -- --nocapture
cargo test -p polyalpha-cli backtest_rust -- --nocapture
python3 -m unittest scripts.tests.test_python_cli_report_layer
python3 -m unittest scripts.tests.test_backtest_compare_report
```

Expected: Rust replay, Python report layer, and monitor-facing diagnostics all pass.

- [ ] **Step 5: Commit verification artifacts and plan progress update**

```bash
git add docs/superpowers/plans/2026-04-01-band-policy-and-rejection-diagnostics.md
git commit -m "docs: record band policy diagnostics verification steps"
```
