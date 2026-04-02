# Monitor tier and retention display (Monitor UI)

## Product intent
- Keep the current market table structure but make product-facing tier/retention states explicit inside the existing status column.
- Operators must instantly distinguish markets in the tradeable pool, focus pool, and observation pool, and understand why any focus-market is retained (position, residual recovery, close, delta rebalance, or force exit).
- Sorting should prioritize tradeable → focus (retention path first) → observation.
- The new `MarketView` fields (`market_tier`, `retention_reason`, `last_focus_at_ms`, `last_tradeable_at_ms`) must feed both the textual labels and ranking logic.

## Approaches
1. Reuse existing status column but keep reliance on `execution_pool`/`evaluable_status`. Fast but does not align with new tier taxonomy.
2. Add extra columns or badges alongside the table. Clearer but breaks the stable layout requirement.
3. Compose the status column text from `MarketTier` plus `MarketRetentionReason`, update coloring and sorting to align with the brand-new semantics, and keep the existing structure. (Recommended because it keeps the table stable while achieving visibility and logic changes.)

## Design decisions
- Sort markets by `MarketTier` priority (tradeable > focus > observation), breaking ties by retention reason rank (focus markets with reasons stay ahead of observation) and older heuristics (rank, position, sample freshness).
- Format the status column as: `交易池`, `重点池`, or `观察池`, and append `/` plus the retention reason label (compact) when `retention_reason != none` (e.g., `重点池/持仓`).
- Use color cues per tier: green for `交易池`, cyan/yellow for `重点池`, and dark gray for `观察池`. When a focus market has a retention reason, respect the existing residual color mapping to emphasize active responsibilities.
- The `last_focus_at_ms` and `last_tradeable_at_ms` fields will not surface in the UI directly but can optionally be used to refine future visual updates; not required for the current text-plus-color approach.

## Testing & verification
- Update the `sample_market` helper in `monitor/ui.rs` so new fields have defined defaults, ensuring tests hit the formatting logic.
- Run the existing `monitor::ui` tests that inspect the status column (the ones that verify `format_market_status_compact` output) to ensure the composed strings stay compact and correct.

## Next steps after approval
- Apply the sorting changes and status formatting in `monitor/ui.rs` (and update `state.rs` only if new helper state info is needed).
- If additional UI coverage is desired later, surface timestamps or trend indicators derived from `last_focus_at_ms`/`last_tradeable_at_ms` in detail panes.
