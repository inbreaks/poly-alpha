# CEX Venue Update Dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove per-market CEX WS fanout so one venue-level CEX update enters the runtime once and then refreshes all attached markets locally.

**Architecture:** Introduce venue-level CEX market-data events in core/data, then teach the engine and paper runtime to expand those updates locally against the attached symbols. Keep Polymarket and strategy semantics unchanged, and preserve existing per-market monitor outputs.

**Tech Stack:** Rust, Tokio broadcast channels, existing `MarketDataEvent` pipeline, paper runtime, `SimpleAlphaEngine`.

---

### Task 1: Add Failing Tests For Venue-Level CEX Events

**Files:**
- Modify: `crates/polyalpha-data/src/normalizer.rs`
- Modify: `crates/polyalpha-engine/src/lib.rs`

- [ ] Write a failing normalizer test showing one CEX venue update no longer expands into multiple market events.
- [ ] Run the targeted normalizer test and confirm it fails for the expected reason.
- [ ] Write a failing engine test showing one venue-level CEX update refreshes multiple markets sharing the same CEX symbol.
- [ ] Run the targeted engine test and confirm it fails for the expected reason.

### Task 2: Add Venue-Level CEX Event Types And Publishing Path

**Files:**
- Modify: `crates/polyalpha-core/src/event.rs`
- Modify: `crates/polyalpha-data/src/normalizer.rs`
- Modify: `crates/polyalpha-data/src/manager.rs`

- [ ] Add venue-level CEX event variants for orderbook, trade, and funding updates.
- [ ] Change the CEX normalizer path to emit one venue-level event per raw venue update.
- [ ] Keep Polymarket normalization unchanged.
- [ ] Run the targeted normalizer tests and confirm they pass.

### Task 3: Teach Engine And Paper Runtime To Expand Venue Events Locally

**Files:**
- Modify: `crates/polyalpha-core/src/types/signal.rs`
- Modify: `crates/polyalpha-engine/src/lib.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] Add minimal output-merging support for batched engine processing.
- [ ] Update the engine to map one venue-level CEX event to all attached market symbols.
- [ ] Update single-market and multi-market paper WS handlers to consume venue-level CEX events without reintroducing channel fanout.
- [ ] Run targeted engine and paper tests and confirm they pass.

### Task 4: Verify Build And Regression Surface

**Files:**
- Modify: `crates/polyalpha-core/src/event.rs`
- Modify: `crates/polyalpha-data/src/normalizer.rs`
- Modify: `crates/polyalpha-data/src/manager.rs`
- Modify: `crates/polyalpha-engine/src/lib.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`

- [ ] Run the targeted CEX venue-event tests.
- [ ] Run the existing diagnostics and WS monotonicity tests.
- [ ] Build `polyalpha-cli` and confirm the tree compiles cleanly.
