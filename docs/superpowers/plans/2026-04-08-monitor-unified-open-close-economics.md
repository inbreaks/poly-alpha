# Monitor Unified Open/Close Economics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give monitor one operator-facing economics vocabulary for plan actions and close-now position outcomes without changing strategy logic.

**Architecture:** Reuse existing `TradePlan` raw/planned edge and cost fields to derive a gross/cost/net summary inside event details, and compute the position panel's exit-now gross value directly from the existing realized-plus-mark fields so the UI can show gross/cost/net without expanding shared schema.

**Tech Stack:** Rust workspace (`polyalpha-core`, `polyalpha-cli`), monitor TUI tests, monitor state builders, execution planner preview path.

---

## File Structure

- Modify: `crates/polyalpha-cli/src/paper.rs`
  Responsibility: derive normalized plan economics summary fields.
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
  Responsibility: render the revised position exit-now labels and keep event-detail ordering intuitive.

### Task 1: Add Failing UI Coverage

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`

- [ ] **Step 1: Add a failing test for the position detail panel showing `立即平仓毛值`, `平仓预估成本`, and `立即平仓净值`**
- [ ] **Step 2: Add a failing test for event-detail ordering that expects `计划毛EdgeUSD`, `计划费用USD`, and `计划净EdgeUSD` ahead of lower-level plan fields**
- [ ] **Step 3: Run the targeted UI tests and confirm they fail for the expected missing labels/order**

### Task 2: Derive Economics Values

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Add a failing paper test that expects trade-plan event details to include normalized plan gross/cost/net values**
- [ ] **Step 2: Run the targeted paper test and confirm it fails**
- [ ] **Step 3: Implement the helper for total planned costs and the derived summary fields**
- [ ] **Step 4: Re-run the targeted paper test and confirm it passes**

### Task 3: Render And Verify

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Update the position detail panel labels and values**
- [ ] **Step 2: Re-run the targeted UI and paper tests and confirm they pass**
- [ ] **Step 3: Run broader `polyalpha-cli` coverage before claiming completion**
