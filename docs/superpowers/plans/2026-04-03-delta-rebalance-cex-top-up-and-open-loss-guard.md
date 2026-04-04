# Delta Rebalance CEX Top-Up And Open Loss Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop valid Poly-driven positions from being flattened by hedge drift, and prevent thin-book Poly entries from opening with large immediate mark-to-market losses.

**Architecture:** Keep `delta_rebalance` as a distinct runtime intent, but extend it with enough residual snapshot data for the planner to top up or trim only the CEX leg. In the open path, keep the existing orderbook-aware estimators, but add an explicit instant-loss sizing guard so planner output is bounded by executable loss tolerance rather than blindly consuming the full USD budget.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-executor`, `polyalpha-cli`), existing planner/dry-run helpers, paper runtime tests, targeted Cargo test runs.

---

## File Structure

- Modify: `crates/polyalpha-core/src/types/planning.rs`
  Responsibility: Extend `DeltaRebalance` with residual snapshot data and expose a dedicated planner rejection code for excessive instant-open loss.
- Modify: `crates/polyalpha-core/src/config.rs`
  Responsibility: Add a basis-strategy knob for maximum allowed instant-open loss as a fraction of the planned budget.
- Modify: `crates/polyalpha-cli/src/runtime.rs`
  Responsibility: Update runtime intent builders so open intents and delta-rebalance intents carry the new planner inputs.
- Modify: `crates/polyalpha-cli/src/markets.rs`
  Responsibility: Seed the new basis-strategy defaults used by local runtime and tests.
- Modify: `crates/polyalpha-cli/src/paper.rs`
  Responsibility: Compute signed delta-rebalance snapshots from live positions, emit the richer `DeltaRebalance` intent, and keep paper/runtime regression coverage green.
- Modify: `crates/polyalpha-executor/src/planner.rs`
  Responsibility: Route `DeltaRebalance` through CEX-only top-up logic, add instant-open loss-bounded sizing, and cover both behaviors with planner tests.
- Verify: `crates/polyalpha-executor/src/manager.rs`
  Responsibility: Confirm execution still handles zero-Poly-share CEX-only plans correctly and does not emit false `TradeClosed` events when a symbol remains open.

### Task 1: Preserve `delta_rebalance` As CEX-Only Hedge Repair

**Files:**
- Modify: `crates/polyalpha-core/src/types/planning.rs`
- Modify: `crates/polyalpha-cli/src/runtime.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-executor/src/planner.rs`
- Test: `crates/polyalpha-executor/src/planner.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Add a failing planner regression test for CEX-only delta rebalance**

```rust
#[test]
fn delta_rebalance_prefers_cex_top_up_without_touching_poly() {
    let planner = ExecutionPlanner::default();
    let mut context = sample_context();
    context.current_poly_yes_shares = Decimal::new(800, 0);
    context.current_cex_net_qty = Decimal::new(-990, 2);

    let intent = PlanningIntent::DeltaRebalance {
        schema_version: 1,
        intent_id: "intent-delta-1".to_owned(),
        correlation_id: "corr-delta-1".to_owned(),
        symbol: Symbol::new("btc-price-only"),
        residual_snapshot: ResidualSnapshot {
            schema_version: 1,
            residual_delta: 0.08,
            planned_cex_qty: CexBaseQty(Decimal::new(998, 2)),
            current_poly_yes_shares: PolyShares(Decimal::new(800, 0)),
            current_poly_no_shares: PolyShares::ZERO,
            preferred_cex_side: OrderSide::Sell,
        },
        target_residual_delta_max: 0.05,
        target_shock_loss_max: 50.0,
    };

    let plan = planner.plan(&intent, &context).unwrap();

    assert_eq!(plan.intent_type, "delta_rebalance");
    assert_eq!(plan.poly_planned_shares, PolyShares::ZERO);
    assert_eq!(plan.cex_side, OrderSide::Sell);
    assert!(plan.cex_planned_qty.0 > Decimal::ZERO);
}
```

- [ ] **Step 2: Run the targeted planner test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-executor delta_rebalance_prefers_cex_top_up_without_touching_poly -- --nocapture
```

Expected: compile failure because `DeltaRebalance` does not yet carry `residual_snapshot`, or runtime failure because the planner still routes `delta_rebalance` through `plan_flatten`.

- [ ] **Step 3: Add the minimal runtime/planner plumbing**

Implementation notes:
- Extend `PlanningIntent::DeltaRebalance` in `crates/polyalpha-core/src/types/planning.rs` to carry a `ResidualSnapshot`.
- Keep `DeltaRebalance` as its own intent type and reason code; do not alias it to `ResidualRecovery`.
- In `crates/polyalpha-cli/src/paper.rs`, replace the current absolute-only residual helper with a snapshot helper that returns:
  - unsigned residual magnitude
  - preferred CEX side for top-up or trim
  - current Poly Yes/No shares
  - last planned CEX quantity when available
- Update `delta_rebalance_intent_for_symbol` in `crates/polyalpha-cli/src/runtime.rs` and its paper wrapper to build the richer intent.
- In `crates/polyalpha-executor/src/planner.rs`, route `PlanningIntent::DeltaRebalance` to a dedicated helper that:
  - reuses `plan_cex_top_up(...)` when the CEX top-up is viable
  - preserves `intent_type = "delta_rebalance"` and `priority = "delta_rebalance"`
  - only falls back to `plan_flatten(...)` when CEX-only repair is impossible

- [ ] **Step 4: Add a failing paper/runtime regression that delta rebalance no longer implies flatten**

```rust
#[tokio::test]
async fn delta_rebalance_keeps_symbol_open_when_cex_only_top_up_is_viable() {
    let settings = test_settings_with_price_filter(None, None);
    let market = settings.markets[0].clone();
    let registry = SymbolRegistry::new(settings.markets.clone());
    let (provider, _executor, mut execution) = build_paper_execution_stack(
        &settings,
        &registry,
        1_700_000_000_000,
        RuntimeExecutionMode::Paper,
        None,
    )
    .await
    .expect("paper execution stack");
    seed_runtime_books(&provider, &registry, 2);

    let mut engine = SimpleAlphaEngine::with_markets(
        build_paper_engine_config(&settings),
        settings.markets.clone(),
    );
    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    risk.on_fill(&poly_fill(TokenSide::Yes))
        .await
        .expect("seed poly fill");
    let mut stats = PaperStats::default();
    let mut recent_events = VecDeque::new();
    let mut trade_book = TradeBook::default();
    let mut no_audit = None;
    trade_book.register_open_signal(&open_candidate(TokenSide::Yes), 0, 1.0, 0.25);

    maybe_execute_delta_rebalance(
        &settings,
        &HashMap::from([(
            market.symbol.clone(),
            ObservedState {
                cex_mid: Some(Decimal::new(100_000, 0)),
                ..ObservedState::default()
            },
        )]),
        &market.symbol,
        &mut engine,
        &mut execution,
        &mut risk,
        &mut stats,
        &mut recent_events,
        &mut trade_book,
        &mut no_audit,
        70_000,
    )
    .await
    .expect("delta rebalance should execute");

    assert!(risk.position_tracker().symbol_has_open_position(&market.symbol));
    assert!(!recent_events.iter().any(|event| event.summary.contains("已平仓")));
}
```

- [ ] **Step 5: Run the targeted planner and paper regressions**

Run:

```bash
cargo test -p polyalpha-executor delta_rebalance_prefers_cex_top_up_without_touching_poly -- --nocapture
cargo test -p polyalpha-cli delta_rebalance_emits_intent_when_threshold_exceeded -- --nocapture
cargo test -p polyalpha-cli delta_rebalance_keeps_symbol_open_when_cex_only_top_up_is_viable -- --nocapture
```

Expected: PASS. The planner should emit zero-Poly-share CEX-only plans for viable delta rebalances, and the paper runtime should keep the symbol open after a successful top-up.

### Task 2: Add Strategy-Level Instant-Open Loss Configuration

**Files:**
- Modify: `crates/polyalpha-core/src/config.rs`
- Modify: `crates/polyalpha-cli/src/markets.rs`
- Modify: `crates/polyalpha-cli/src/runtime.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-core/src/config.rs`

- [ ] **Step 1: Add a failing config serialization regression**

```rust
#[test]
fn basis_strategy_config_defaults_open_instant_loss_pct() {
    let config: BasisStrategyConfig = serde_json::from_value(serde_json::json!({
        "entry_z_score_threshold": "4.0",
        "exit_z_score_threshold": "0.5",
        "rolling_window_secs": 36000,
        "min_warmup_samples": 600,
        "min_basis_bps": "50.0",
        "max_position_usd": "200",
        "delta_rebalance_threshold": "0.05",
        "delta_rebalance_interval_secs": 60
    }))
    .expect("config should deserialize");

    assert_eq!(config.max_open_instant_loss_pct_of_budget, Decimal::new(4, 2));
}
```

- [ ] **Step 2: Run the targeted config test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-core basis_strategy_config_defaults_open_instant_loss_pct -- --nocapture
```

Expected: compile failure because `BasisStrategyConfig` does not yet define `max_open_instant_loss_pct_of_budget`.

- [ ] **Step 3: Add the minimal config field and defaults**

Implementation notes:
- Add `max_open_instant_loss_pct_of_budget: Decimal` to `BasisStrategyConfig`.
- Add a serde default helper returning `Decimal::new(4, 2)` so the default is `0.04` of planned budget.
- Seed the new field in:
  - `crates/polyalpha-cli/src/markets.rs`
  - `crates/polyalpha-cli/src/runtime.rs` test settings
  - `crates/polyalpha-cli/src/paper.rs` test settings
- Keep the initial scope global; do not add per-market overrides in this change.

- [ ] **Step 4: Re-run the targeted config test**

Run:

```bash
cargo test -p polyalpha-core basis_strategy_config_defaults_open_instant_loss_pct -- --nocapture
```

Expected: PASS.

### Task 3: Bound Open Size By Instant Executable Loss Instead Of Full Budget

**Files:**
- Modify: `crates/polyalpha-core/src/types/planning.rs`
- Modify: `crates/polyalpha-executor/src/planner.rs`
- Test: `crates/polyalpha-executor/src/planner.rs`

- [ ] **Step 1: Add a failing reason-code regression for the new planner rejection**

```rust
#[test]
fn plan_rejection_reason_exposes_open_instant_loss_too_large_code() {
    assert_eq!(
        PlanRejectionReason::OpenInstantLossTooLarge.code(),
        "open_instant_loss_too_large"
    );
}
```

- [ ] **Step 2: Add a failing planner regression that thin Poly books shrink the open**

```rust
#[test]
fn planner_shrinks_open_size_when_instant_loss_exceeds_budget_fraction() {
    let planner = ExecutionPlanner::default();
    let mut intent = sample_open_intent();
    let PlanningIntent::OpenPosition { candidate, .. } = &mut intent else {
        panic!("expected open intent");
    };
    candidate.risk_budget_usd = 200.0;

    let mut context = sample_context();
    context.poly_yes_book.bids = vec![PriceLevel {
        price: Price(Decimal::new(21, 2)),
        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(50, 0))),
    }];
    context.poly_yes_book.asks = vec![
        PriceLevel {
            price: Price(Decimal::new(21, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(50, 0))),
        },
        PriceLevel {
            price: Price(Decimal::new(25, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(5_000, 0))),
        },
    ];

    let plan = planner.plan(&intent, &context).unwrap();

    assert!(plan.poly_max_cost_usd.0 < Decimal::from(200u32));
    assert!(plan.poly_planned_shares.0 < Decimal::new(800, 0));
}
```

- [ ] **Step 3: Add a failing planner regression that impossible thin-book entries are rejected**

```rust
#[test]
fn planner_rejects_open_when_every_executable_size_breaks_instant_loss_cap() {
    let planner = ExecutionPlanner::default();
    let intent = sample_open_intent();
    let mut context = sample_context();
    context.poly_yes_book.bids = vec![PriceLevel {
        price: Price(Decimal::new(10, 2)),
        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(1, 0))),
    }];
    context.poly_yes_book.asks = vec![PriceLevel {
        price: Price(Decimal::new(90, 2)),
        quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
    }];

    let rejection = planner.plan(&intent, &context).unwrap_err();

    assert_eq!(rejection.reason, PlanRejectionReason::OpenInstantLossTooLarge);
}
```

- [ ] **Step 4: Run the targeted planner tests and confirm they fail**

Run:

```bash
cargo test -p polyalpha-core plan_rejection_reason_exposes_open_instant_loss_too_large_code -- --nocapture
cargo test -p polyalpha-executor planner_shrinks_open_size_when_instant_loss_exceeds_budget_fraction -- --nocapture
cargo test -p polyalpha-executor planner_rejects_open_when_every_executable_size_breaks_instant_loss_cap -- --nocapture
```

Expected: compile failure for the new rejection reason and planner failures because open sizing still consumes the full budget when planned edge remains positive.

- [ ] **Step 5: Implement the smallest loss-bounded sizing loop**

Implementation notes:
- Add `OpenInstantLossTooLarge` to `PlanRejectionReason` with stable code `open_instant_loss_too_large`.
- In `ExecutionPlanner::plan_open`, compute `allowed_instant_loss_usd` from:
  - `budget_cap_usd`
  - `settings.strategy.basis.max_open_instant_loss_pct_of_budget`
- Introduce a helper that binary-searches a reduced Poly budget cap between `0` and the original `budget_cap_usd`.
- For each candidate budget:
  - call `estimate_poly_open(...)`
  - derive `poly_planned_shares`
  - estimate the matching CEX hedge
  - compute `instant_open_loss_usd = poly_friction + cex_friction + poly_fee + cex_fee`
- Keep the largest candidate budget whose instant loss stays within the allowed threshold.
- If no positive executable size satisfies the constraint, reject with `OpenInstantLossTooLarge`.
- Preserve the existing positive-edge, residual-delta, and shock-loss checks after the new size has been selected.

- [ ] **Step 6: Re-run the targeted planner regressions plus nearby coverage**

Run:

```bash
cargo test -p polyalpha-core plan_rejection_reason_exposes_open_instant_loss_too_large_code -- --nocapture
cargo test -p polyalpha-executor planner_shrinks_open_size_when_instant_loss_exceeds_budget_fraction -- --nocapture
cargo test -p polyalpha-executor planner_rejects_open_when_every_executable_size_breaks_instant_loss_cap -- --nocapture
cargo test -p polyalpha-executor open_plan_uses_executable_price_as_buy_cap -- --nocapture
cargo test -p polyalpha-executor planner_planned_edge_includes_fees_and_funding -- --nocapture
```

Expected: PASS. Thin-book entries should either shrink below the raw `200 USD` budget or reject cleanly with `open_instant_loss_too_large`.

### Task 4: Full Regression And Runtime Verification

**Files:**
- Verify: `crates/polyalpha-executor/src/manager.rs`
- Verify: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Run execution-manager coverage for zero-Poly-share recovery/top-up paths**

Run:

```bash
cargo test -p polyalpha-executor recovery_plan_prefers_cex_top_up_when_recovery_book_has_liquidity -- --nocapture
cargo test -p polyalpha-executor recovery_plan_is_executed_to_a_follow_up_result -- --nocapture
cargo test -p polyalpha-executor close_position_intent_submits_flattening_orders -- --nocapture
```

Expected: PASS. Zero-Poly-share CEX-only recovery plans should still execute cleanly, and `TradeClosed` should remain reserved for the explicit flatten path when the symbol actually becomes flat.

- [ ] **Step 2: Run focused CLI/package regressions**

Run:

```bash
cargo test -p polyalpha-cli delta_rebalance_emits_intent_when_threshold_exceeded -- --nocapture
cargo test -p polyalpha-cli delta_rebalance_is_throttled_within_interval -- --nocapture
cargo test -p polyalpha-cli execute_intent_trigger_keeps_runtime_alive_on_close_rejection -- --nocapture
```

Expected: PASS. Existing paper-runtime delta-rebalance and degraded-close behavior should remain intact.

- [ ] **Step 3: Run full package verification**

Run:

```bash
cargo test -p polyalpha-executor
cargo test -p polyalpha-cli
cargo build -p polyalpha-cli
```

Expected: all tests pass and the CLI builds cleanly.

## Deferred Follow-Up

- Round-trip entry/exit loss guard for thin Poly books remains intentionally out of scope for this plan.
- Per-market overrides for instant-open loss tolerance remain intentionally out of scope for this plan.
