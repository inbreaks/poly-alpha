# Strict Open Hedge Invariant Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OpenPosition` planning reject any basis-style open that cannot produce a meaningful CEX hedge after planner sizing, while keeping old signal-era hedge guards only as a compatibility safety fuse.

**Architecture:** Extend the shared planning rejection taxonomy with a dedicated machine-readable reason for “open plan has no effective hedge,” then enforce that invariant inside `ExecutionPlanner::plan_open`. Keep the existing legacy `zero_cex_hedge_qty` checks in old paper/manager paths unchanged so they remain a downstream fuse instead of the source of truth.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-executor`, `polyalpha-cli`), inline Rust unit tests, existing planner/executor test helpers.

---

## File Structure

- Modify: `crates/polyalpha-core/src/types/planning.rs`
  Responsibility: Add a dedicated planner rejection reason and stable reason code for open plans that cannot produce a valid CEX hedge.
- Modify: `crates/polyalpha-executor/src/planner.rs`
  Responsibility: Add planner-level open rejection for zero / non-hedgeable CEX quantity and regression tests proving the new invariant.
- Verify: `crates/polyalpha-executor/src/manager.rs`
  Responsibility: Ensure legacy signal-era `zero_cex_hedge_qty` fuse coverage stays intact after planner changes.

### Task 1: Add Planner Rejection Taxonomy For Non-Hedgeable Opens

**Files:**
- Modify: `crates/polyalpha-core/src/types/planning.rs`
- Test: `crates/polyalpha-core/src/types/planning.rs`

- [ ] **Step 1: Add a failing reason-code test**

```rust
#[test]
fn plan_rejection_reason_exposes_zero_cex_hedge_qty_code() {
    assert_eq!(
        PlanRejectionReason::ZeroCexHedgeQty.code(),
        "zero_cex_hedge_qty"
    );
}
```

- [ ] **Step 2: Run the targeted core test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-core plan_rejection_reason_exposes_zero_cex_hedge_qty_code -- --nocapture
```

Expected: compile failure because `ZeroCexHedgeQty` does not exist yet.

- [ ] **Step 3: Add the minimal enum variant and code mapping**

Implementation notes:
- Add `ZeroCexHedgeQty` to `PlanRejectionReason`.
- Map it to the stable machine-readable code `zero_cex_hedge_qty`.
- Keep existing reason codes unchanged.

- [ ] **Step 4: Re-run the targeted core test**

Run:

```bash
cargo test -p polyalpha-core plan_rejection_reason_exposes_zero_cex_hedge_qty_code -- --nocapture
```

Expected: PASS.

### Task 2: Enforce The Invariant In `ExecutionPlanner::plan_open`

**Files:**
- Modify: `crates/polyalpha-executor/src/planner.rs`
- Test: `crates/polyalpha-executor/src/planner.rs`

- [ ] **Step 1: Add a failing planner regression test for zero-delta opens**

```rust
#[test]
fn planner_rejects_open_when_candidate_has_zero_cex_hedge_qty() {
    let planner = ExecutionPlanner::default();
    let mut intent = sample_open_intent();
    let PlanningIntent::OpenPosition { candidate, .. } = &mut intent else {
        panic!("expected open intent");
    };
    candidate.delta_estimate = 0.0;

    let rejection = planner.plan(&intent, &sample_context()).unwrap_err();

    assert_eq!(rejection.reason, PlanRejectionReason::ZeroCexHedgeQty);
    assert!(rejection.detail.contains("cannot produce"));
}
```

- [ ] **Step 2: Run the targeted planner test and confirm it fails**

Run:

```bash
cargo test -p polyalpha-executor planner_rejects_open_when_candidate_has_zero_cex_hedge_qty -- --nocapture
```

Expected: test failure because the planner currently allows `delta_estimate = 0.0` opens.

- [ ] **Step 3: Add the minimal planner guard**

Implementation notes:
- In `ExecutionPlanner::plan_open`, reject `OpenPosition` plans when the candidate cannot produce a meaningful hedge.
- Cover both cases:
  - `candidate.delta_estimate.abs() <= 0`
  - `exact_cex_qty <= 0` after final Poly sizing
- Return `PlanRejectionReason::ZeroCexHedgeQty`.
- Keep the existing residual/shock checks for non-zero hedge paths unchanged.
- Do not remove legacy signal-era guards.

- [ ] **Step 4: Re-run planner tests plus nearby regression coverage**

Run:

```bash
cargo test -p polyalpha-executor planner_rejects_open_when_candidate_has_zero_cex_hedge_qty -- --nocapture
cargo test -p polyalpha-executor open_plan_uses_executable_price_as_buy_cap -- --nocapture
cargo test -p polyalpha-executor replan_from_execution_rescales_child_hedge_to_actual_fill -- --nocapture
```

Expected: PASS.

### Task 3: Verify Legacy Fuse Still Exists But Is No Longer Primary Truth

**Files:**
- Verify: `crates/polyalpha-executor/src/manager.rs`

- [ ] **Step 1: Run the existing legacy fuse regression test**

Run:

```bash
cargo test -p polyalpha-executor invalid_open_signal_returns_rejected_order_without_submission -- --nocapture
```

Expected: PASS with `zero_cex_hedge_qty` still returned for the old signal path.

- [ ] **Step 2: Summarize the boundary**

Verification notes:
- Planner rejects invalid `OpenPosition` plans before execution.
- Old `ArbSignalEvent` guard still rejects invalid legacy signals if any old entry path remains reachable.
- No code change is required in `manager.rs` unless the test reveals a regression.
