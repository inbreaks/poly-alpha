# Monitor Position Exit-Now Economics Design

## Goal

Expose a clearer per-position economics view in monitor so operators can distinguish:

- current mark-to-market PnL
- already realized PnL/costs on the still-open position
- additional cost to close right now
- estimated net PnL if the position is closed immediately

This is a visibility change only. It does not change open / close / risk gating.

## Current Problem

The current monitor position detail shows:

- `poly_pnl_usd`
- `cex_pnl_usd`
- `total_pnl_usd = poly_pnl_usd + cex_pnl_usd`

That is only gross mark PnL for the still-open legs. It does not include:

- fees already paid on entry or partial closes
- realized PnL already accumulated on partially reduced legs
- funding already applied to runtime equity
- the additional friction/fees required to close now

As a result, operators can misread a positive mark PnL as "closing now is still net profitable".

## Approved Scope

Add the following fields to per-position monitor data:

- `realized_pnl_so_far_usd`
  Signed realized PnL already attached to the open position legs plus recorded funding adjustments for that symbol.
- `close_cost_preview_usd`
  Additional close cost if a full close intent were previewed now. This uses the existing close planner and should equal the absolute value of the preview close plan's negative `planned_edge_usd`.
- `exit_now_net_pnl_usd`
  Estimated total net PnL if the symbol is fully closed now.

Keep existing fields unchanged:

- `poly_pnl_usd`
- `cex_pnl_usd`
- `total_pnl_usd`

`total_pnl_usd` remains the current gross mark PnL to preserve existing monitor ordering and behavior.

## Computation Rules

### 1. Mark PnL

Keep existing formulas:

- `poly_pnl_usd = signed mark PnL for the active Poly leg`
- `cex_pnl_usd = signed mark PnL for the CEX leg`
- `total_pnl_usd = poly_pnl_usd + cex_pnl_usd`

### 2. Realized PnL So Far

Use the open-position truth already available from runtime state:

- sum `position.realized_pnl` across all open legs for the symbol
- add any funding adjustments recorded for that symbol while the trade remains open

This captures real executed fees and any realized leg PnL without inventing a separate entry-cost reconstruction path.

### 3. Close Cost Preview

Build a full close preview with the existing execution manager:

- create a `ClosePosition` intent with ratio `1.0`
- call `preview_intent_detailed`
- if preview succeeds:
  - `close_cost_preview_usd = max(0, -plan.planned_edge_usd)`
- if preview fails:
  - leave close preview as `None`

This ensures the monitor uses the same planner truth as actual close execution.

### 4. Exit-Now Net PnL

If close preview succeeds:

- `exit_now_net_pnl_usd = realized_pnl_so_far_usd + total_pnl_usd - close_cost_preview_usd`

If close preview fails:

- leave `exit_now_net_pnl_usd = None`

This avoids claiming a precise exit-now number when the system cannot currently produce a valid close plan.

## UI Behavior

Update the position detail panel to show:

- `Polymarket盈亏`
- `交易所盈亏`
- `盯市总盈亏`
- `已实现盈亏`
- `平仓预估成本`
- `立即平仓净值`

For unavailable preview values, show a clear fallback such as `--`.

## Non-Goals

- do not change auto-exit triggers
- do not add close profitability gating
- do not change performance aggregation
- do not change market sorting semantics

## Affected Files

- `crates/polyalpha-core/src/socket/message.rs`
  Extend `PositionView` with new economics fields.
- `crates/polyalpha-cli/src/paper.rs`
  Track per-symbol funding adjustments while positions remain open, compute realized-so-far and close preview values, and populate the new `PositionView` fields.
- `crates/polyalpha-cli/src/monitor/ui.rs`
  Render the new position detail lines and add regression coverage.

