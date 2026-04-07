# Polymarket WS Runtime Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add minimal runtime diagnostics so a `256 markets + single Polymarket WS` run can show whether freshness degradation comes from Polymarket inbound flow, broadcast receiver lag, or event-loop drain pressure.

**Architecture:** Keep the current single-connection topology unchanged. Add lightweight counters in the Polymarket WS ingest path and multi-market WS runtime loop, then expose them through audit snapshots and monitor runtime so `fresh` and `fresh2` can be compared with evidence.

**Tech Stack:** Rust, Tokio broadcast channels, serde, existing audit/monitor state models

---

### Task 1: Add failing tests for diagnostics surfacing

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Write failing tests**
  Add focused unit tests for:
  - Polymarket WS diagnostics accumulation
  - market-data lag/drain accounting
  - audit snapshot exposing the new counters
  - monitor runtime exposing the new counters

- [ ] **Step 2: Run targeted tests to verify they fail**
  Run: `cargo test -p polyalpha-cli diagnostics_ -- --nocapture`
  Expected: compile or assertion failures because the new fields and helpers do not exist yet.

### Task 2: Implement minimal diagnostics plumbing

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-audit/src/model.rs`

- [ ] **Step 1: Add Polymarket ingest counters**
  Count text frames, `price_change` messages, book messages, and emitted orderbook updates.

- [ ] **Step 2: Add market-data receiver lag/drain counters**
  Count lag events, lagged message totals, last per-tick drain volume, and max per-tick drain volume in `run_paper_multi_ws_mode`.

- [ ] **Step 3: Expose counters in audit and monitor runtime**
  Extend the audit counter snapshot and monitor runtime structs, then populate them from runtime state.

### Task 3: Verify and prepare rerun guidance

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-audit/src/model.rs`

- [ ] **Step 1: Run targeted tests**
  Run: `cargo test -p polyalpha-cli diagnostics_ -- --nocapture`
  Expected: PASS

- [ ] **Step 2: Run broader build verification**
  Run: `cargo build -p polyalpha-cli`
  Expected: PASS

- [ ] **Step 3: Use the counters on the next `fresh` rerun**
  Compare `fresh` vs `fresh2` by inspecting:
  - `counters.polymarket_ws_*`
  - `counters.market_data_rx_lag_*`
  - `runtime.market_data_tick_drain_*`

