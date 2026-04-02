# Market Tier/Retention Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface the new `MarketView` tier and retention fields inside the existing status/sorting system so the monitor clearly distinguishes tradeable, focus, and observation pools with retention reason tags, using only the current table column layout.

**Architecture:** Tier and retention metadata live on each `MarketView`; the monitor UI composes its sort order, status text, and colors from those fields alongside existing metrics. Sorting/formatting helpers aggregate this logic so the main rendering pipeline remains unchanged.

**Tech Stack:** `polyalpha-cli` TUI (Rust + `ratatui`), `polyalpha-core` shared domain models, `cargo test -p polyalpha-cli monitor::ui` for targeted verification.

---

### Task 1: Prioritize markets by tier and retention in `sorted_markets`

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs:417-1900` (helper section around sorting)
- Test: `crates/polyalpha-cli/src/monitor/ui.rs` tests module (add new test during helper block)

- [ ] **Step 1: Add failing regression test**

```rust
    #[test]
    fn sorted_markets_prioritizes_tier_and_retention() {
        let mut state = TuiState::default();
        state.monitor.markets = vec![
            sample_market("obs", None).with_tier(MarketTier::Observation),
            sample_market("focus_retention", None)
                .with_tier(MarketTier::Focus)
                .with_retention(MarketRetentionReason::HasPosition),
            sample_market("tradeable", None)
                .with_tier(MarketTier::Tradeable),
        ];
        let sorted = sorted_markets(&state.monitor.markets);
        assert_eq!(sorted[0].symbol, "tradeable");
        assert_eq!(sorted[1].symbol, "focus_retention");
        assert_eq!(sorted[2].symbol, "obs");
    }
```

Expected failure: test fails because `sorted_markets` currently ignores `market_tier` and `retention_reason`.

- [ ] **Step 2: Run the failing test**

Run: `cargo test -p polyalpha-cli monitor::ui::sorted_markets_prioritizes_tier_and_retention -- --nocapture`
Expected: fail with assertion `actual sequence differs` because sorting still uses old heuristics.

- [ ] **Step 3: Update `market_availability_rank`/`sorted_markets`**

Add helper that maps `MarketTier` to priority (`Tradeable>Focus>Observation`) and when tiers match, uses `retention_reason` ordering (focus markets with reasons stay above bare focus/observation). Embed it into the existing compare chain before the old heuristics so the new fields drive order. Code example:

```rust
fn tier_priority(market: &MarketView) -> i32 {
    match market.market_tier {
        MarketTier::Tradeable => 300,
        MarketTier::Focus => 200 + retention_priority(market.retention_reason),
        MarketTier::Observation => 100,
    }
}

fn retention_priority(reason: MarketRetentionReason) -> i32 {
    match reason {
        MarketRetentionReason::None => 0,
        MarketRetentionReason::HasPosition => 3,
        MarketRetentionReason::ResidualRecovery => 2,
        MarketRetentionReason::CloseInProgress => 1,
        MarketRetentionReason::DeltaRebalance => 1,
        MarketRetentionReason::ForceExit => 4,
    }
}
```

Integrate by comparing `tier_priority` values before previous `market_rank` logic.

- [ ] **Step 4: Run the test again

Run: `cargo test -p polyalpha-cli monitor::ui::sorted_markets_prioritizes_tier_and_retention`
Expected: PASS with the asserted ordering.

- [ ] **Step 5: Commit**

```bash
git add crates/polyalpha-cli/src/monitor/ui.rs
git commit -m "chore: align market sorting with tier retention"
```

### Task 2: Display tier and retention labels/colors in status column

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs:2320-2410` (status formatting helpers)
- Test: `crates/polyalpha-cli/src/monitor/ui.rs` tests module (update existing status tests to expect new labels)

- [ ] **Step 1: Update `format_market_status_compact` to compose tier+reason**

Replace the current status string builder with logic that first formats the tier label (`market.market_tier.label_zh()`). If `retention_reason != MarketRetentionReason::None`, append `/` plus `retention_reason.compact_label_zh()` (but skip `--` for `None`). Example snippet:

```rust
fn format_market_status_compact(market: &MarketView) -> String {
    let tier_label = match market.market_tier {
        MarketTier::Tradeable => "交易池",
        MarketTier::Focus => "重点池",
        MarketTier::Observation => "观察池",
    };
    let retention_label = match market.retention_reason {
        MarketRetentionReason::None => None,
        reason => Some(reason.compact_label_zh()),
    };
    match retention_label {
        Some(reason) => format!("{tier_label}/{reason}"),
        None => tier_label.to_owned(),
    }
}
```

- [ ] **Step 2: Adjust `market_status_color`**

Map `MarketTier::Tradeable` to `Color::Green`, focus to `Color::Cyan` (escalated to `Color::Yellow` as needed) and observation to `Color::DarkGray`. If there is also an active `residual_state` or `awaiting_cancel_ack`, preserve existing colors (red/yellow) to highlight issues. Example snippet:

```rust
fn market_status_color(market: &MarketView) -> Color {
    if market.awaiting_cancel_ack || market.symbol_blacklisted { return Color::Red; }
    if market.reconcile_only || market.residual_state.is_some() {
        return residual_state_color(market);
    }
    match market.market_tier {
        MarketTier::Tradeable => Color::Green,
        MarketTier::Focus => {
            if market.retention_reason != MarketRetentionReason::None {
                Color::Yellow
            } else {
                Color::Cyan
            }
        }
        MarketTier::Observation => Color::DarkGray,
    }
}
```

- [ ] **Step 3: Update existing status label tests**

Ensure tests that check `format_market_status_compact` now expect the tier/retention label. For example replace `"可评估/双腿新鲜"` with `"交易池"` (or `"观察池"`) as appropriate (anticipating new output). Provide inline snippet showing new expected strings. The tests should still assert there are no width issues.

- [ ] **Step 4: Run the targeted status tests**

Run: `cargo test -p polyalpha-cli monitor::ui::market_table_uses_compact_status_labels`
Expected: PASS with the updated expectations.

- [ ] **Step 5: Commit**

```bash
git add crates/polyalpha-cli/src/monitor/ui.rs
git commit -m "feat: show tier retention status in market column"
```

### Task 3: Align helpers/tests with new tiers/retention metadata

**Files:**
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs:2750-2790` (test helper definitions)

- [ ] **Step 1: Extend `sample_market` to set new fields by default**

Ensure the helper sets `market_tier`, `retention_reason`, and the timestamps so tests covering both tiers and status text exercise the new format. Update the helper with snippet:

```rust
            market_tier: MarketTier::Tradeable,
            retention_reason: MarketRetentionReason::None,
            last_focus_at_ms: None,
            last_tradeable_at_ms: None,
```

- [ ] **Step 2: Run the relevant tests**

Run: `cargo test -p polyalpha-cli monitor::ui::sample_market` (if such test exists) or simply rerun the previous targeted tests to ensure helper updates do not break anything.

- [ ] **Step 3: Commit helper update**

```bash
git add crates/polyalpha-cli/src/monitor/ui.rs
git commit -m "test: keep sample market aligned with tier retention fields"
```

### Task 4: Smoke in-scope monitor UI tests after changes

**Files:** None (test command only)
- [ ] **Step 1: Run aggregate monitor UI tests**

Run: `cargo test -p polyalpha-cli monitor::ui -q`
Expected: PASS. This ensures sorting/status changes and helper updates integrate without regression.

- [ ] **Step 2: Record test results** (no command output required here, but note PASS in final report)
