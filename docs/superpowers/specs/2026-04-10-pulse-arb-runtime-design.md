# Pulse Arb Runtime Design

## Goal

Add a new independent short-horizon strategy runtime that harvests short-lived Polymarket sentiment overshoots by:

- using options-derived fair probability as the pricing anchor
- using Binance USD-M perpetuals for fast delta hedging
- managing a full 15-minute trading session lifecycle with maker exit, pegging, rehedging, timeout, and emergency flatten logic

The MVP must trade only:

- `BTC`
- `ETH`

The architecture must preserve native extension points for future support of:

- `SOL`
- `XRP`
- additional options anchor providers
- additional emergency hedge venues

without requiring a large-scale rewrite of the runtime.

## Thesis

This runtime is not a variant of the existing `price-only basis` strategy.

It is a separate strategy family with a different economic thesis:

- `Polymarket` is the sentiment venue where retail overshoot occurs
- `Options anchor` is the fair-value venue that expresses a more disciplined probability surface
- `Binance perp` is the execution venue that neutralizes directional delta quickly

The runtime therefore must be:

- session-centric, not signal-centric
- options-anchor-driven, not realized-vol-only-driven
- execution-lifecycle-aware, not just entry/exit-band-aware

## Approved Scope

### MVP Scope

- runtime mode: `paper` and `live-mock` only
- assets: `BTC`, `ETH`
- options anchor provider: `Deribit`
- hedge venue: `Binance USD-M perpetual`
- Polymarket execution: independent runtime-owned order lifecycle
- session max holding time: `15 minutes`
- maker exit management: included
- pegging: included
- delta rehedging during the session: included
- emergency flatten: included
- structured audit and monitor support: included

### Required Future Extension Capability

The MVP design must explicitly preserve extension points for:

- `SOL` and `XRP`
- `Binance Options` as an anchor provider
- asset-specific routing policies
- secondary emergency hedge venues such as `OKX`

Future extension must require only:

- adding a new provider implementation
- adding or updating routing config
- adding asset-level policy config

Future extension must not require:

- replacing the session state machine
- rewriting the event pricer interface
- redesigning the audit schema
- merging this runtime into the old basis strategy path

## Non-Goals

- do not retrofit this strategy into the existing `basis` engine path
- do not change the meaning of the existing `TradePlan`-driven basis runtime
- do not implement live money trading in the MVP
- do not promise `SOL` or `XRP` tradability in the MVP
- do not implement full queue reconstruction in the MVP
- do not require a full volatility-surface calibration framework in the MVP

## High-Level Architecture

Introduce a new strategy stack, tentatively named `pulse_arb`, with its own runtime and session model.

Recommended crate/module split:

- `polyalpha-data`
  Continues to provide normalized market data feeds for Polymarket, Binance perp, and options anchor venues.
- `polyalpha-pulse` new crate
  Owns the strategy-specific runtime, pricing, session state machine, execution logic, and audit model.
- `polyalpha-cli`
  Adds dedicated entrypoints for `paper pulse` and `live-mock pulse`.
- `polyalpha-executor`
  Reused only as a bottom-layer venue adapter capability where practical, not as the strategy lifecycle owner.

### Core Runtime Components

- `AnchorProvider`
  Normalized interface for retrieving options-anchor market data.
- `AnchorRouter`
  Maps an asset to its configured anchor provider.
- `EventPricer`
  Converts normalized anchor data plus market-rule metadata into fair probability and event Greeks.
- `PulseDetector`
  Decides whether a fresh Polymarket overshoot is tradable after friction and session reserves.
- `SessionFSM`
  Owns one active trading session from creation to closure.
- `HedgeExecutor`
  Manages Binance perp hedge open and rehedge actions.
- `ExitManager`
  Manages Poly maker take-profit, pegging, timeout chase, and forced exit.
- `PulseAuditSink`
  Writes session-centric audit artifacts and summary events.

This split is the main mechanism that prevents future large refactors.

## Provider And Routing Model

The design must treat anchor providers and asset routing as first-class runtime objects.

### Provider Registry

The runtime owns a provider registry keyed by logical provider id.

MVP provider instances:

- `deribit_primary`

Future provider instances:

- `binance_options_primary`
- additional providers as needed

Every provider must expose the same normalized output schema so that:

- routing changes do not affect session logic
- session logic does not care whether the anchor came from Deribit or Binance Options

### Asset Routing

Each asset resolves through routing config:

- `anchor_provider`
- `hedge_venue`
- `enabled`
- optional asset-level overrides

MVP routing:

- `BTC -> Deribit anchor + Binance perp`
- `ETH -> Deribit anchor + Binance perp`

Planned future routing:

- `SOL -> Binance Options anchor + Binance perp`
- `XRP -> Binance Options anchor + Binance perp`

The runtime must allow an asset to be disabled without affecting other assets.

## Normalized Anchor Output

`AnchorProvider` must output a normalized `AnchorSnapshot` rather than raw venue-specific IV fields.

Required fields:

- `asset`
- `provider_id`
- `ts_ms`
- `index_price`
- `expiry_ts_ms`
- `atm_iv`
- `local_surface_points`
- `quality_metrics`

Each `local_surface_point` should contain:

- `strike`
- `bid_iv`
- `ask_iv`
- `mark_iv`
- `delta`
- `gamma`
- `best_bid`
- `best_ask`

Quality metrics must include enough detail to disable trading when the anchor is not trustworthy.

Minimum quality dimensions:

- anchor age
- bid/ask spread quality
- strike coverage
- local liquidity quality
- expiry mismatch
- required Greeks availability

## Pricing Model

This runtime prices binary event claims, not vanilla options.

### Supported Market Rules

- `Above K`
- `Below K`
- `Between [K1, K2)`

### Fair Probability

For each market:

- `fair_prob_yes` is the risk-neutral probability of the event payout being `1`
- `fair_prob_no = 1 - fair_prob_yes`

The anchor provider does not directly supply event probability.

Instead:

- the provider supplies local options-surface information
- `EventPricer` reconstructs a local risk-neutral view for the relevant expiry
- the market rule maps that view into event probability

### MVP Pricing Complexity

The MVP must use a local-chain approximation, not a full global surface calibration.

Recommended MVP approach:

- choose the nearest acceptable expiry to the Polymarket settlement
- use strikes around the relevant event threshold
- interpolate locally rather than calibrating a full volatility surface
- for `Between`, use both boundary strikes and compute interval probability

This keeps the interface forward-compatible with a future richer surface model.

## Delta And Rehedge Model

The runtime must not use vanilla-option `N(d1)` as the hedge quantity for a binary claim.

Instead:

- define event value as `fair_prob_yes`
- compute event delta numerically:
  - price the event at `S + dS`
  - price the event at `S - dS`
  - use central difference

This yields:

- `event_delta_yes`
- `event_delta_no = -event_delta_yes`

Target hedge quantity:

- `target_hedge_qty = poly_shares * event_delta`

### Rehedge Trigger

Rehedging must be driven by target hedge drift, not by raw spot movement alone.

Minimum trigger inputs:

- current target hedge quantity
- current actual hedge quantity
- delta-drift threshold
- gamma-sensitive zone handling near critical strike levels
- throttle interval

The runtime should allow tighter rehedge thresholds when price moves into a gamma-sensitive zone.

## Entry Economics

Entry must be based on executable session economics, not on a raw probability gap alone.

The runtime should compute a session-level quantity such as:

- `net_session_edge`

This should include:

- Poly executable entry VWAP using top `N` levels
- expected Binance hedge execution cost
- fees
- estimated slippage
- perp/index basis penalty
- reserve for expected rehedges
- reserve for timeout exit

The session should open only if:

- anchor quality passes
- Poly and hedge data freshness pass
- required depth passes
- `net_session_edge > configured_entry_threshold`

## Session-Centric Runtime

The central runtime object should be `PulseSession`, not a basis-style open candidate.

Each session contains:

- `session_id`
- market and asset identity
- token side
- entry snapshot
- target deadline
- target take-profit state
- current hedge state
- current lifecycle state

### Session State Machine

Recommended states:

- `Idle`
- `PreTradeAudit`
- `PolyOpening`
- `HedgeOpening`
- `MakerExitWorking`
- `Pegging`
- `Rehedging`
- `EmergencyHedge`
- `EmergencyFlatten`
- `ChasingExit`
- `Closed`

### Lifecycle Summary

1. `PreTradeAudit`
   Freeze all required input snapshots and compute final session economics.
2. `PolyOpening`
   Open the Poly leg aggressively with full-fill semantics.
3. `HedgeOpening`
   Immediately hedge on Binance perp after confirmed Poly fill.
4. `MakerExitWorking`
   Post a Poly maker take-profit order.
5. `Pegging`
   Cancel and replace the maker exit order when anchor and market movement materially invalidate the old quote.
6. `Rehedging`
   Adjust the Binance hedge if event delta drifts enough during the session.
7. `Emergency*`
   Recover from legging failures or force flatten if the risk thesis breaks.
8. `ChasingExit`
   On timeout, aggressively close the remaining position instead of waiting for maker fill.
9. `Closed`
   Finalize economics and audit.

This state machine must remain independent from the existing basis runtime semantics.

## Execution Semantics

### Poly Open

The Poly open leg should use a full-fill-first aggressive execution style with:

- explicit max average price
- explicit max shares / budget
- fail-fast behavior if the pulse cannot be captured cleanly

### Hedge Open

The Binance hedge leg should use a bounded aggressive execution style.

Recommended MVP choice:

- aggressive limit + `IOC`

Reason:

- bounded slippage is more important than uncapped market execution for a short-horizon edge-capture strategy
- this avoids pretending that `MARKET + IOC` is a distinct venue capability

### Maker Exit

The first exit path is always:

- post-only maker take-profit on Polymarket

### Timeout Exit

At the holding deadline:

- cancel the working maker order
- execute aggressive exit logic
- prioritize closure and risk release over waiting for a better fill

## Configuration Model

The runtime must have its own top-level config domain such as:

- `[strategy.pulse_arb]`

Recommended config groups:

- `runtime`
- `session`
- `entry`
- `rehedge`
- `providers`
- `routing`
- `asset_policy`

### Required Config Capabilities

- enable/disable by strategy
- enable/disable by asset
- provider-specific quality thresholds
- routing by asset
- per-asset session and economics overrides
- explicit max holding time
- explicit timeout exit mode
- explicit rehedge throttles and thresholds

MVP behavior should be narrow, but config schema should already be wide enough for future providers and assets.

## Observability And Audit

This runtime needs a session-centric observability model.

### Primary Audit Object

Introduce a session-level audit record such as:

- `PulseSessionRecord`

It should contain:

- `session_header`
- `entry_snapshot`
- `lifecycle_events`
- `final_economics`

### Entry Timing Metrics

The audit schema must record raw timing samples, not just aggregated latency scores.

Required raw timing fields:

- `anchor_ts_ms`
- `anchor_recv_ts_ms`
- `anchor_latency_ms`
- `poly_book_ts_ms`
- `poly_book_recv_ts_ms`
- `poly_latency_ms`
- `hedge_book_ts_ms`
- `hedge_book_recv_ts_ms`
- `hedge_latency_ms`
- `decision_ts_ms`
- `decision_skew_ms`
- `submit_poly_order_ts_ms`
- `submit_delay_ms`
- `poly_fill_latency_ms`
- `hedge_fill_latency_ms`

Offline reports may then compute derived statistics such as:

- latency percentiles
- win rate by latency bucket
- information lag entropy

The raw samples must be kept even if the entropy-style summary is not rendered in the MVP UI.

### Maker-Quality Metrics

The MVP should use stable proxy metrics rather than claiming perfect queue reconstruction.

Required proxy fields:

- `tp_post_price`
- `tp_post_ts_ms`
- `distance_to_mid_bps_at_post`
- `distance_to_touch_ticks_at_post`
- `relative_order_age_ms`
- `peg_count`
- `cancel_replace_count`
- `maker_fill_wait_ms`
- `deadline_hit`
- `post_tp_adverse_move_bps_1s`
- `post_tp_adverse_move_bps_5s`
- `fill_then_reversal_flag`

### Raw Tape Retention For Future Queue Reconstruction

Even though the MVP uses proxy metrics, the audit layer must retain enough raw market tape to support future queue-reconstruction work without redesigning the schema.

Required retained data:

- top-`N` Poly book snapshots at post/cancel/replace/fill boundaries
- relevant Poly book deltas during maker-order lifetime
- relevant trade summaries during maker-order lifetime
- pre- and post-peg local book context

This is a required future-proofing constraint.

### Failure Taxonomy

The runtime must emit machine-readable failure codes.

Minimum codes:

- `anchor_stale`
- `anchor_surface_insufficient`
- `anchor_expiry_mismatch`
- `poly_depth_insufficient`
- `hedge_depth_insufficient`
- `poly_open_rejected`
- `hedge_open_rejected`
- `maker_post_rejected`
- `peg_rate_limited`
- `rehedge_blocked`
- `deadline_exit_triggered`
- `emergency_flatten_triggered`
- `asset_disabled`
- `hedge_disconnect`
- `poly_disconnect`

## Monitor Model

This strategy should appear in monitor as a dedicated strategy view, not as a lightly renamed basis panel.

Recommended monitor sections:

- active session list
- single-session detail
- asset health panel

### Active Session List

Each row should show:

- symbol
- side
- state
- remaining holding time
- current delta drift
- current session net PnL
- current maker working status

### Session Detail

Should show:

- entry snapshot
- anchor provider and quality
- current TP price
- peg history
- rehedge history
- timeout countdown
- emergency state history

### Asset Health

Should show:

- enabled / degraded / disabled state
- current anchor provider
- anchor quality summary
- current disable reason if any
- recent session failure rate
- timeout ratio
- average rehedge count

## Health, Degrade, And Disable Semantics

Each asset must support independent lifecycle gating:

- `Enabled`
- `Degraded`
- `Disabled`

### Degrade

Use when the runtime should keep observing but trade more conservatively.

Examples:

- anchor quality weak but not fully broken
- repeated timeout exits
- rehedge drag consistently high

### Disable

Use when new sessions must stop.

Examples:

- anchor disconnected or stale
- insufficient strike coverage
- hedge venue unavailable
- repeated severe legging failures

The runtime must not silently switch providers or trade with a weak fallback path unless routing config explicitly allows that behavior.

## Reused Infrastructure

This runtime may reuse:

- normalized data feeds
- shared market metadata and market-rule parsing
- shared order adapter primitives
- shared audit/artifact storage framework
- shared monitor socket infrastructure

This runtime must not inherit the old basis strategy's:

- signal semantics
- exit-band semantics
- planner truth model
- monitor vocabulary

## MVP Success Criteria

The MVP should be considered complete when all of the following are true:

- `BTC` and `ETH` can run in `paper` and `live-mock`
- the runtime uses `Deribit` as the options anchor for both assets
- the runtime hedges on `Binance USD-M perp`
- every trade is represented as a full `PulseSession`
- maker exit, pegging, rehedge, timeout, and emergency flatten are all observable in audit
- the monitor can show active sessions and asset health
- the audit schema already preserves enough raw tape for future queue reconstruction
- the config schema already supports future provider and routing expansion

## Future Expansion Path

The intended next expansion path after the MVP is:

1. add `BinanceOptionsAnchorProvider`
2. add routing entries for future assets such as `SOL` and `XRP`
3. add asset-specific quality and economics overrides
4. optionally add emergency secondary hedge venues
5. optionally upgrade from proxy maker metrics to queue-reconstruction analytics

The expansion path must not change:

- `PulseSession`
- `SessionFSM`
- `EventPricer` interface
- `AnchorProvider` interface
- audit schema shape

## Affected Areas

- new crate: `crates/polyalpha-pulse`
  Own pulse runtime, pricing, session FSM, execution logic, audit model.
- `crates/polyalpha-data`
  Add any missing normalized feed support required by Deribit options and Binance perp.
- `crates/polyalpha-core`
  Add shared config and any cross-runtime types that truly belong in core.
- `crates/polyalpha-cli`
  Add strategy-specific runtime entrypoints, monitor integration, and config loading.
- storage / monitor plumbing
  Reuse shared frameworks but add pulse-specific schemas and views.
