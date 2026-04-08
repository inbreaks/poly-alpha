# Monitor Position Exit-Now Economics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show per-position close preview cost and exit-now net PnL in monitor without changing trading decisions.

**Architecture:** Extend `PositionView` with new economics fields, accumulate per-symbol funding adjustments in `TradeBook`, then compute close-preview-based values during monitor render request construction using the existing execution planner. Keep existing gross mark PnL fields and behavior unchanged.

**Tech Stack:** Rust workspace (`polyalpha-core`, `polyalpha-cli`), existing monitor UI tests, execution planner preview path.

---

## File Structure

- Modify: `crates/polyalpha-core/src/socket/message.rs`
  Responsibility: Add new serialized `PositionView` fields and update affected tests.
- Modify: `crates/polyalpha-cli/src/paper.rs`
  Responsibility: Store per-symbol funding adjustments, compute per-position realized-so-far / close preview / exit-now net values, and thread mutable execution access into monitor render request building.
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
  Responsibility: Render the new position economics fields and cover them with UI regression tests.

### Task 1: Add Failing Monitor Detail Regression

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`

- [ ] **Step 1: Write a failing UI test for the new economics labels and values**
- [ ] **Step 2: Run the targeted UI test and confirm it fails because `PositionView` lacks the new fields or the detail panel does not render them**

### Task 2: Extend Shared PositionView Schema

**Files:**
- Modify: `crates/polyalpha-core/src/socket/message.rs`

- [ ] **Step 1: Add failing construction/serde coverage for the new `PositionView` fields**
- [ ] **Step 2: Run the targeted core test and confirm it fails**
- [ ] **Step 3: Add `realized_pnl_so_far_usd`, `close_cost_preview_usd`, and `exit_now_net_pnl_usd` to `PositionView`**
- [ ] **Step 4: Re-run the targeted core test and confirm it passes**

### Task 3: Compute Position Exit-Now Economics

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Add a failing paper test covering a symbol with realized leg PnL, funding adjustment, and a successful close preview**
- [ ] **Step 2: Run the targeted paper test and confirm it fails**
- [ ] **Step 3: Add per-symbol funding accumulation to `TradeBook`**
- [ ] **Step 4: Add helpers that sum symbol realized PnL from grouped positions, preview a full close plan, and compute the new `PositionView` fields**
- [ ] **Step 5: Thread mutable `execution` access through monitor render request builders where needed**
- [ ] **Step 6: Re-run the targeted paper test and confirm it passes**

### Task 4: Render And Verify

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`

- [ ] **Step 1: Update the position detail panel to render the new economics rows**
- [ ] **Step 2: Run the targeted UI test and confirm it passes**
- [ ] **Step 3: Run the nearby paper / monitor / core tests to catch schema or UI regressions**

