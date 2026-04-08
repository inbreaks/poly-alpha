# Monitor Unified Open/Close Economics Design

## Goal

Extend the monitor economics view so operators can read both entry and exit actions through one simple lens:

- gross economics
- execution / carry costs
- net economics

This is still a visibility change only. It does not change strategy gating or execution behavior.

## Problem

The monitor currently exposes:

- position outcome values for an already-open trade
- low-level trade-plan cost fields inside event details

But these two surfaces do not use one operator-facing vocabulary. As a result:

- open plans require mentally translating raw/planned edge plus multiple fee fields
- close-now position views show outcome values but not a parallel gross/cost/net shape
- operators cannot quickly compare "is this action good before costs?" vs "is it still good after costs?"

## Approved Scope

### 1. Normalize planned action economics in event details

For any `TradePlan` shown in monitor event details, add derived summary fields:

- `计划毛EdgeUSD`
- `计划费用USD`
- `计划净EdgeUSD`

These fields should be available for:

- open plans
- close plans
- recovery / rebalance plans

The existing detailed fields remain visible underneath them.

### 2. Normalize close-now position economics in position detail

Render the exit-now block as:

- `立即平仓毛值`
- `平仓预估成本`
- `立即平仓净值`

Keep the existing:

- `盯市总盈亏`
- `已实现盈亏`

So the operator can see both the decomposition and its components.

## Computation Rules

### Planned action economics

For a `TradePlan`:

- `计划毛EdgeUSD = raw_edge_usd`
- `计划费用USD = poly_friction_cost_usd + poly_fee_usd + cex_friction_cost_usd + cex_fee_usd + expected_funding_cost_usd + residual_risk_penalty_usd`
- `计划净EdgeUSD = planned_edge_usd`

This gives a stable operator vocabulary while preserving the lower-level fields for debugging.

### Position close-now economics

For an open position:

- `exit_now_gross_pnl_usd = realized_pnl_so_far_usd + total_pnl_usd`
- `close_cost_preview_usd` remains the current close preview cost derived from the preview close plan
- `exit_now_net_pnl_usd = exit_now_gross_pnl_usd - close_cost_preview_usd`

If the close preview fails:

- `close_cost_preview_usd = None`
- `exit_now_net_pnl_usd = None`
- `exit_now_gross_pnl_usd` is still available because it is derived directly from existing monitor fields

## UI Behavior

### Position detail

Show:

- mark legs and mark total
- realized PnL so far
- immediate-exit gross / cost / net

### Event detail

Show the three summary plan economics fields before the lower-level friction / fee breakdown so the operator sees the high-level answer first.

## Non-Goals

- do not preview open plans for every market on every refresh
- do not change open or close gating
- do not alter performance accounting
- do not remove the lower-level detailed trade-plan fields

## Affected Files

- `crates/polyalpha-cli/src/paper.rs`
  Inject normalized plan economics into event details.
- `crates/polyalpha-cli/src/monitor/ui.rs`
  Render the updated exit-now labels using existing position fields and order the new plan economics summary fields ahead of the detailed costs.
