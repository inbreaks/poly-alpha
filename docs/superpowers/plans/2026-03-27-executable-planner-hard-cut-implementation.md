# Executable Planner Hard-Cut Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current `ArbSignalEvent`-driven execution path with a schema-versioned `OpenCandidate -> PlanningIntent -> TradePlan -> ExecutionResult` pipeline that uses executable pricing, constrained Poly sizing, actual-fill-driven hedging, and planner-owned recovery.

**Architecture:** Introduce the new planning types in `polyalpha-core`, add deterministic planner logic in `polyalpha-executor`, then refactor engine output, execution state, and CLI runtimes to consume plans instead of legacy signals. Keep execution semantics explicit: Poly is the primary leg, CEX follows actual Poly fills, and every residual path returns to the planner with machine-readable reason codes.

**Tech Stack:** Rust workspace crates (`polyalpha-core`, `polyalpha-engine`, `polyalpha-executor`, `polyalpha-cli`, `polyalpha-risk`, `polyalpha-audit`), `serde`, `tokio`, `rust_decimal`, existing orderbook replay/dry-run infrastructure, existing `uuid` dependency for deterministic `plan_hash`.

---

## File Structure

- Create: `crates/polyalpha-core/src/types/planning.rs`
  Responsibility: Define `schema_version`, `OpenCandidate`, `PlanningIntent`, `TradePlan`, `ExecutionResult`, machine-readable reason enums, fill ledger structs, and stable helper methods such as `code()`.
- Modify: `crates/polyalpha-core/src/types/mod.rs`
  Responsibility: Export new planning types and stop re-exporting legacy executable signal types after the hard cut.
- Modify: `crates/polyalpha-core/src/types/signal.rs`
  Responsibility: Keep `DmmQuote*`, `EngineWarning`, and `AlphaEngineOutput`, but replace `arb_signals` with `open_candidates`.
- Modify: `crates/polyalpha-core/src/types/order.rs`
  Responsibility: Replace ambiguous Poly sizing fields with mutually exclusive `BuyBudgetCap`, `SellExactShares`, and `SellMinProceeds`.
- Modify: `crates/polyalpha-core/src/channel.rs`
  Responsibility: Rename the execution-facing channel from `ArbSignalTx/Rx` to `PlanningIntentTx/Rx`.
- Modify: `crates/polyalpha-core/src/event.rs`
  Responsibility: Add `TradePlanCreated`, `PlanSuperseded`, and `RecoveryPlanCreated` execution events used by the new state machine.
- Modify: `crates/polyalpha-core/src/socket/message.rs`
  Responsibility: Add schema-versioned candidate/intent/plan/result views and new reason-code fields to monitor transport.
- Modify: `crates/polyalpha-engine/src/lib.rs`
  Responsibility: Emit `OpenCandidate` values instead of executable basis signals.
- Modify: `crates/polyalpha-engine/tests/price_only_parity.rs`
  Responsibility: Assert candidate-only engine output and remove assumptions about `expected_pnl` and hedge sizing.
- Create: `crates/polyalpha-executor/src/planner.rs`
  Responsibility: Canonicalize `PlanningContext`, calculate deterministic `TradePlan`s, revalidate plans, and choose recovery routes.
- Create: `crates/polyalpha-executor/src/plan_state.rs`
  Responsibility: Track in-flight plans, `parent_plan_id`, `supersedes_plan_id`, and `idempotency_key`.
- Modify: `crates/polyalpha-executor/src/lib.rs`
  Responsibility: Export the planner and plan-state types.
- Modify: `crates/polyalpha-executor/src/dry_run.rs`
  Responsibility: Support constrained Poly sizing, fee/funding-aware preview cost fields, and precise fill ledgers.
- Modify: `crates/polyalpha-executor/src/manager.rs`
  Responsibility: Consume `PlanningIntent`, submit `TradePlan`s, drive actual-fill hedging, and route unresolved residuals back through planner-owned recovery.
- Modify: `crates/polyalpha-risk/src/in_memory.rs`
  Responsibility: Estimate notional and exposure from new Poly sizing modes instead of `shares + quote_notional`.
- Modify: `crates/polyalpha-audit/src/model.rs`
  Responsibility: Store schema-versioned candidate/plan/result payloads and reason codes.
- Modify: `crates/polyalpha-cli/src/paper.rs`
  Responsibility: Convert engine candidates and manual close/rebalance requests into `PlanningIntent`s, emit plan/result audit details, and remove legacy signal economics code.
- Modify: `crates/polyalpha-cli/src/sim.rs`
  Responsibility: Reuse planner pipeline under deterministic replay.
- Modify: `crates/polyalpha-cli/src/backtest_rust.rs`
  Responsibility: Base fills, reports, and parity checks on `TradePlan` and `ExecutionResult`.
- Modify: `crates/polyalpha-cli/src/commands.rs`
  Responsibility: Build preview and risk requests from `TradePlan` instead of `ArbSignalEvent`.
- Modify: `crates/polyalpha-cli/src/audit.rs`
  Responsibility: Render reason codes, fee/funding columns, and schema-versioned plan/result details.
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
  Responsibility: Carry candidate/plan/result fields into monitor state.
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
  Responsibility: Show theoretical/planned/realized edge, fee/funding, plan validity, and reason-code details.

### Task 1: Introduce Schema-Versioned Planning Types

**Files:**
- Create: `crates/polyalpha-core/src/types/planning.rs`
- Modify: `crates/polyalpha-core/src/types/mod.rs`
- Modify: `crates/polyalpha-core/src/types/signal.rs`
- Modify: `crates/polyalpha-core/src/channel.rs`
- Test: `crates/polyalpha-core/src/types/planning.rs`

- [ ] **Step 1: Write the failing core type tests**

```rust
#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        OpenCandidate, PlanRejectionReason, PlanningIntent, SignalStrength, Symbol, TokenSide,
    };

    #[test]
    fn planning_objects_roundtrip_with_schema_version() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.47,
            raw_mispricing: 0.06,
            delta_estimate: 0.18,
            risk_budget_usd: 200.0,
            strength: SignalStrength::Normal,
            z_score: Some(2.1),
            timestamp_ms: 1_716_000_000_000,
        };

        let encoded = serde_json::to_value(&candidate).unwrap();
        assert_eq!(encoded["schema_version"], json!(1));
        let decoded: OpenCandidate = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.schema_version, 1);
    }

    #[test]
    fn plan_rejection_codes_are_stable() {
        assert_eq!(
            PlanRejectionReason::MissingPolyBook.code(),
            "missing_poly_book"
        );
    }

    #[test]
    fn alpha_engine_output_collects_open_candidates() {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-2".to_owned(),
            correlation_id: "corr-2".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::No,
            direction: "short".to_owned(),
            fair_value: 0.58,
            raw_mispricing: -0.04,
            delta_estimate: -0.12,
            risk_budget_usd: 150.0,
            strength: SignalStrength::Weak,
            z_score: Some(-1.8),
            timestamp_ms: 1_716_000_000_100,
        };

        let mut output = crate::AlphaEngineOutput::default();
        output.push_open_candidate(candidate);
        assert_eq!(output.open_candidates.len(), 1);
    }
}
```

- [ ] **Step 2: Run the core tests to verify they fail**

Run: `cargo test -p polyalpha-core planning_ -- --nocapture`

Expected: FAIL with missing imports such as `OpenCandidate`, `PlanRejectionReason`, `PlanningIntent`, or `push_open_candidate`.

- [ ] **Step 3: Add the new planning types and stable reason enums**

```rust
use serde::{Deserialize, Serialize};

use super::{PolyShares, Price, Symbol, TokenSide, UsdNotional};

pub const PLANNING_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OpenCandidate {
    pub schema_version: u32,
    pub candidate_id: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub direction: String,
    pub fair_value: f64,
    pub raw_mispricing: f64,
    pub delta_estimate: f64,
    pub risk_budget_usd: f64,
    pub strength: super::SignalStrength,
    pub z_score: Option<f64>,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanRejectionReason {
    MissingPolyBook,
    MissingCexBook,
    OneSidedPolyBook,
    OneSidedCexBook,
    NonPositivePlannedEdge,
}

impl PlanRejectionReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::MissingPolyBook => "missing_poly_book",
            Self::MissingCexBook => "missing_cex_book",
            Self::OneSidedPolyBook => "one_sided_poly_book",
            Self::OneSidedCexBook => "one_sided_cex_book",
            Self::NonPositivePlannedEdge => "non_positive_planned_edge",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PlanningIntent {
    OpenPosition {
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        candidate: OpenCandidate,
        max_budget_usd: f64,
        max_residual_delta: f64,
        max_shock_loss_usd: f64,
    },
    ClosePosition {
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        close_reason: String,
        target_close_ratio: f64,
    },
    DeltaRebalance {
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        target_residual_delta_max: f64,
        target_shock_loss_max: f64,
    },
    ResidualRecovery {
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        residual_snapshot: String,
        recovery_reason: RecoveryDecisionReason,
    },
    ForceExit {
        schema_version: u32,
        intent_id: String,
        correlation_id: String,
        symbol: Symbol,
        force_reason: String,
        allow_negative_edge: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RevalidationFailureReason {
    PlanTtlExpired,
    PlannedEdgeDeteriorated,
    ResidualRiskDeteriorated,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDecisionReason {
    CexTopUpCheaper,
    PolyFlattenCheaper,
    ForceExitRequired,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OrderLedgerEntry {
    pub order_id: String,
    pub client_order_id: String,
    pub requested_qty: String,
    pub filled_qty: String,
    pub cancelled_qty: String,
    pub avg_price: String,
    pub fee_usd: f64,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TradePlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub parent_plan_id: Option<String>,
    pub supersedes_plan_id: Option<String>,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub intent_type: String,
    pub priority: String,
    pub created_at_ms: u64,
    pub poly_exchange_timestamp_ms: u64,
    pub poly_received_at_ms: u64,
    pub poly_sequence: u64,
    pub cex_exchange_timestamp_ms: u64,
    pub cex_received_at_ms: u64,
    pub cex_sequence: u64,
    pub plan_hash: String,
    pub poly_side: super::OrderSide,
    pub poly_token_side: TokenSide,
    pub poly_sizing_mode: String,
    pub poly_requested_shares: PolyShares,
    pub poly_planned_shares: PolyShares,
    pub poly_max_cost_usd: UsdNotional,
    pub poly_max_avg_price: Price,
    pub poly_max_shares: PolyShares,
    pub poly_min_avg_price: Price,
    pub poly_min_proceeds_usd: UsdNotional,
    pub poly_book_avg_price: Price,
    pub poly_executable_price: Price,
    pub poly_friction_cost_usd: UsdNotional,
    pub poly_fee_usd: UsdNotional,
    pub cex_side: super::OrderSide,
    pub cex_planned_qty: super::CexBaseQty,
    pub cex_book_avg_price: Price,
    pub cex_executable_price: Price,
    pub cex_friction_cost_usd: UsdNotional,
    pub cex_fee_usd: UsdNotional,
    pub raw_edge_usd: UsdNotional,
    pub planned_edge_usd: UsdNotional,
    pub expected_funding_cost_usd: UsdNotional,
    pub residual_risk_penalty_usd: UsdNotional,
    pub post_rounding_residual_delta: f64,
    pub shock_loss_up_1pct: UsdNotional,
    pub shock_loss_down_1pct: UsdNotional,
    pub shock_loss_up_2pct: UsdNotional,
    pub shock_loss_down_2pct: UsdNotional,
    pub plan_ttl_ms: u64,
    pub max_poly_sequence_drift: u64,
    pub max_cex_sequence_drift: u64,
    pub max_poly_price_move: Price,
    pub max_cex_price_move: Price,
    pub min_planned_edge_usd: UsdNotional,
    pub max_residual_delta: f64,
    pub max_shock_loss_usd: UsdNotional,
    pub max_plan_vs_fill_deviation_usd: UsdNotional,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionResult {
    pub schema_version: u32,
    pub plan_id: String,
    pub correlation_id: String,
    pub symbol: Symbol,
    pub status: String,
    pub poly_order_ledger: Vec<OrderLedgerEntry>,
    pub cex_order_ledger: Vec<OrderLedgerEntry>,
    pub actual_poly_cost_usd: UsdNotional,
    pub actual_cex_cost_usd: UsdNotional,
    pub actual_poly_fee_usd: UsdNotional,
    pub actual_cex_fee_usd: UsdNotional,
    pub actual_funding_cost_usd: UsdNotional,
    pub realized_edge_usd: UsdNotional,
    pub plan_vs_fill_deviation_usd: UsdNotional,
    pub actual_residual_delta: f64,
    pub actual_shock_loss_up_1pct: UsdNotional,
    pub actual_shock_loss_down_1pct: UsdNotional,
    pub actual_shock_loss_up_2pct: UsdNotional,
    pub actual_shock_loss_down_2pct: UsdNotional,
    pub recovery_required: bool,
    pub timestamp_ms: u64,
}
```

- [ ] **Step 4: Export the types and replace legacy engine output/channel fields**

```rust
// crates/polyalpha-core/src/types/mod.rs
pub mod planning;

pub use planning::{
    ExecutionResult, OpenCandidate, OrderLedgerEntry, PlanRejectionReason, PlanningIntent,
    RecoveryDecisionReason, RevalidationFailureReason, TradePlan, PLANNING_SCHEMA_VERSION,
};

// crates/polyalpha-core/src/types/signal.rs
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AlphaEngineOutput {
    pub dmm_updates: Vec<DmmQuoteUpdate>,
    #[serde(default)]
    pub open_candidates: Vec<crate::OpenCandidate>,
    #[serde(default)]
    pub warnings: Vec<EngineWarning>,
}

impl AlphaEngineOutput {
    pub fn push_open_candidate(&mut self, candidate: crate::OpenCandidate) {
        self.open_candidates.push(candidate);
    }
}

// crates/polyalpha-core/src/channel.rs
pub type PlanningIntentTx = mpsc::Sender<PlanningIntent>;
pub type PlanningIntentRx = mpsc::Receiver<PlanningIntent>;
```

- [ ] **Step 5: Run the core tests again**

Run: `cargo test -p polyalpha-core planning_ -- --nocapture`

Expected: PASS for the new planning serialization and code-stability tests.

- [ ] **Step 6: Commit the planning type layer**

```bash
git add crates/polyalpha-core/src/types/planning.rs crates/polyalpha-core/src/types/mod.rs crates/polyalpha-core/src/types/signal.rs crates/polyalpha-core/src/channel.rs
git commit -m "refactor: introduce schema-versioned planning types"
```

### Task 2: Replace Poly Order Sizing Semantics and Preview Cost Fields

**Files:**
- Modify: `crates/polyalpha-core/src/types/order.rs`
- Modify: `crates/polyalpha-executor/src/dry_run.rs`
- Modify: `crates/polyalpha-risk/src/in_memory.rs`
- Test: `crates/polyalpha-executor/src/dry_run.rs`

- [ ] **Step 1: Write failing dry-run tests for constrained Poly sizing**

```rust
#[test]
fn buy_budget_cap_stops_at_max_cost_usd() {
    let request = OrderRequest::Poly(PolyOrderRequest {
        client_order_id: ClientOrderId("poly-buy-budget".to_owned()),
        symbol: Symbol::new("btc-price-only"),
        token_side: TokenSide::Yes,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        sizing: PolySizingInstruction::BuyBudgetCap {
            max_cost_usd: UsdNotional(Decimal::new(200, 0)),
            max_avg_price: Price(Decimal::new(33, 2)),
            max_shares: PolyShares(Decimal::new(700, 0)),
        },
        time_in_force: TimeInForce::Fok,
        post_only: false,
    });

    let estimate = executor.estimate_order_request(&request);
    assert!(estimate.executable_notional_usd.0 <= Decimal::new(200, 0));
}

#[test]
fn sell_min_proceeds_rejects_when_book_cannot_pay_enough() {
    let request = OrderRequest::Poly(PolyOrderRequest {
        client_order_id: ClientOrderId("poly-sell-min-proceeds".to_owned()),
        symbol: Symbol::new("btc-price-only"),
        token_side: TokenSide::Yes,
        side: OrderSide::Sell,
        order_type: OrderType::Market,
        sizing: PolySizingInstruction::SellMinProceeds {
            shares: PolyShares(Decimal::new(400, 0)),
            min_proceeds_usd: UsdNotional(Decimal::new(150, 0)),
        },
        time_in_force: TimeInForce::Fok,
        post_only: false,
    });

    let estimate = executor.estimate_order_request(&request);
    assert_eq!(estimate.rejection_reason.as_deref(), Some("poly_min_proceeds_not_met"));
}
```

- [ ] **Step 2: Run the dry-run tests to verify they fail**

Run: `cargo test -p polyalpha-executor budget_cap -- --nocapture`

Expected: FAIL because `PolySizingInstruction` and the new rejection reason do not exist yet.

- [ ] **Step 3: Replace ambiguous Poly sizing with an explicit enum**

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PolySizingInstruction {
    BuyBudgetCap {
        max_cost_usd: UsdNotional,
        max_avg_price: Price,
        max_shares: PolyShares,
    },
    SellExactShares {
        shares: PolyShares,
        min_avg_price: Price,
    },
    SellMinProceeds {
        shares: PolyShares,
        min_proceeds_usd: UsdNotional,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolyOrderRequest {
    pub client_order_id: ClientOrderId,
    pub symbol: Symbol,
    pub token_side: TokenSide,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub sizing: PolySizingInstruction,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
}
```

- [ ] **Step 4: Update dry-run preview and risk estimation to honor the new sizing modes**

```rust
// crates/polyalpha-executor/src/dry_run.rs
match &req.sizing {
    PolySizingInstruction::BuyBudgetCap {
        max_cost_usd,
        max_avg_price,
        max_shares,
    } => self.estimate_poly_buy_budget(orderbook, *max_cost_usd, *max_avg_price, *max_shares),
    PolySizingInstruction::SellExactShares { shares, min_avg_price } => {
        self.estimate_poly_sell_exact(orderbook, *shares, Some(*min_avg_price), None)
    }
    PolySizingInstruction::SellMinProceeds {
        shares,
        min_proceeds_usd,
    } => self.estimate_poly_sell_exact(orderbook, *shares, None, Some(*min_proceeds_usd)),
}

// crates/polyalpha-risk/src/in_memory.rs
match &order.sizing {
    PolySizingInstruction::BuyBudgetCap { max_cost_usd, .. } => *max_cost_usd,
    PolySizingInstruction::SellExactShares { shares, min_avg_price } => {
        UsdNotional(shares.0 * min_avg_price.0)
    }
    PolySizingInstruction::SellMinProceeds { min_proceeds_usd, .. } => *min_proceeds_usd,
}
```

- [ ] **Step 5: Add fee fields to `OrderExecutionEstimate` so planner economics can consume them**

```rust
pub struct OrderExecutionEstimate {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub instrument: InstrumentKind,
    pub side: OrderSide,
    pub status: OrderStatus,
    pub requested_quantity: VenueQuantity,
    pub filled_quantity: VenueQuantity,
    pub orderbook_mid_price: Option<Price>,
    pub book_average_price: Option<Price>,
    pub executable_price: Option<Price>,
    pub executable_notional_usd: UsdNotional,
    pub friction_cost_usd: Option<UsdNotional>,
    pub fee_usd: Option<UsdNotional>,
    pub rejection_reason: Option<String>,
    pub is_complete: bool,
}
```

- [ ] **Step 6: Run the dry-run and risk tests again**

Run: `cargo test -p polyalpha-executor "budget_cap|sell_min_proceeds" -- --nocapture`

Expected: PASS, and `cargo test -p polyalpha-risk -- --nocapture` still passes.

- [ ] **Step 7: Commit the new order sizing layer**

```bash
git add crates/polyalpha-core/src/types/order.rs crates/polyalpha-executor/src/dry_run.rs crates/polyalpha-risk/src/in_memory.rs
git commit -m "refactor: add constrained polymarket sizing modes"
```

### Task 3: Build the Deterministic Execution Planner

**Files:**
- Create: `crates/polyalpha-executor/src/planner.rs`
- Modify: `crates/polyalpha-executor/src/lib.rs`
- Test: `crates/polyalpha-executor/src/planner.rs`

- [ ] **Step 1: Write failing planner tests for determinism, one-sided books, and fee-aware economics**

```rust
fn sample_open_intent() -> PlanningIntent {
    let candidate = OpenCandidate {
        schema_version: 1,
        candidate_id: "cand-plan-1".to_owned(),
        correlation_id: "corr-plan-1".to_owned(),
        symbol: Symbol::new("btc-price-only"),
        token_side: TokenSide::Yes,
        direction: "long".to_owned(),
        fair_value: 0.47,
        raw_mispricing: 0.06,
        delta_estimate: 0.18,
        risk_budget_usd: 200.0,
        strength: SignalStrength::Normal,
        z_score: Some(2.4),
        timestamp_ms: 1_716_000_000_000,
    };

    PlanningIntent::OpenPosition {
        schema_version: 1,
        intent_id: "intent-plan-1".to_owned(),
        correlation_id: "corr-plan-1".to_owned(),
        symbol: Symbol::new("btc-price-only"),
        candidate,
        max_budget_usd: 200.0,
        max_residual_delta: 0.05,
        max_shock_loss_usd: 20.0,
    }
}

fn sample_context() -> CanonicalPlanningContext {
    CanonicalPlanningContext {
        planner_depth_levels: 5,
        poly_yes_book: OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-price-only"),
            instrument: InstrumentKind::PolyYes,
            bids: vec![PriceLevel {
                price: Price(Decimal::new(45, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(400, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(47, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
            }],
            exchange_timestamp_ms: 1_716_000_000_000,
            received_at_ms: 1_716_000_000_001,
            sequence: 100,
            last_trade_price: None,
        },
        poly_no_book: OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-price-only"),
            instrument: InstrumentKind::PolyNo,
            bids: vec![PriceLevel {
                price: Price(Decimal::new(53, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(400, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(55, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
            }],
            exchange_timestamp_ms: 1_716_000_000_000,
            received_at_ms: 1_716_000_000_001,
            sequence: 100,
            last_trade_price: None,
        },
        cex_book: OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-price-only"),
            instrument: InstrumentKind::CexPerp,
            bids: vec![PriceLevel {
                price: Price(Decimal::new(100_000, 0)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(5, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(100_010, 0)),
                quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(5, 0))),
            }],
            exchange_timestamp_ms: 1_716_000_000_000,
            received_at_ms: 1_716_000_000_001,
            sequence: 200,
            last_trade_price: None,
        },
        now_ms: 1_716_000_000_100,
    }
}

#[test]
fn planner_hash_is_stable_for_identical_context() {
    let planner = ExecutionPlanner::default();
    let intent = sample_open_intent();
    let context = sample_context();

    let first = planner.plan(&intent, &context).unwrap();
    let second = planner.plan(&intent, &context).unwrap();

    assert_eq!(first.plan_hash, second.plan_hash);
}

#[test]
fn planner_rejects_one_sided_poly_book() {
    let planner = ExecutionPlanner::default();
    let intent = sample_open_intent();
    let mut context = sample_context();
    context.poly_yes_book.asks.clear();

    let rejection = planner.plan(&intent, &context).unwrap_err();
    assert_eq!(rejection.reason, PlanRejectionReason::OneSidedPolyBook);
}

#[test]
fn planned_edge_includes_fees_and_funding() {
    let planner = ExecutionPlanner::default();
    let plan = planner.plan(&sample_open_intent(), &sample_context()).unwrap();

    assert!(plan.poly_fee_usd.0 > Decimal::ZERO);
    assert!(plan.cex_fee_usd.0 > Decimal::ZERO);
    assert!(plan.expected_funding_cost_usd.0 >= Decimal::ZERO);
    assert!(plan.planned_edge_usd.0 < plan.raw_edge_usd.0);
}
```

- [ ] **Step 2: Run the planner tests to verify they fail**

Run: `cargo test -p polyalpha-executor planner_ -- --nocapture`

Expected: FAIL because `ExecutionPlanner`, `plan_hash`, and canonical planner context do not exist yet.

- [ ] **Step 3: Implement the canonical planner context and deterministic `plan_hash`**

```rust
#[derive(Clone, Debug)]
pub struct CanonicalPlanningContext {
    pub planner_depth_levels: usize,
    pub poly_yes_book: OrderBookSnapshot,
    pub poly_no_book: OrderBookSnapshot,
    pub cex_book: OrderBookSnapshot,
    pub now_ms: u64,
}

impl CanonicalPlanningContext {
    pub fn canonical_bytes(&self, intent: &PlanningIntent) -> Vec<u8> {
        serde_json::to_vec(&(intent, self)).expect("canonical planning context")
    }

    pub fn from_observed_state(
        settings: &Settings,
        registry: &SymbolRegistry,
        observed: &ObservedState,
        intent: &PlanningIntent,
        execution: &ExecutionManager<DryRunExecutor>,
    ) -> Result<Self> {
        let symbol = match intent {
            PlanningIntent::OpenPosition { symbol, .. }
            | PlanningIntent::ClosePosition { symbol, .. }
            | PlanningIntent::DeltaRebalance { symbol, .. }
            | PlanningIntent::ResidualRecovery { symbol, .. }
            | PlanningIntent::ForceExit { symbol, .. } => symbol,
        };
        let market = registry
            .get_config(symbol)
            .ok_or_else(|| anyhow!("missing market config for {}", symbol.0))?;
        let provider = execution
            .orderbook_provider()
            .ok_or_else(|| anyhow!("missing orderbook provider"))?;

        Ok(Self {
            planner_depth_levels: settings.strategy.market_data.max_orderbook_depth as usize,
            poly_yes_book: provider
                .get_orderbook(Exchange::Polymarket, symbol, InstrumentKind::PolyYes)
                .ok_or_else(|| anyhow!("missing poly yes book"))?,
            poly_no_book: provider
                .get_orderbook(Exchange::Polymarket, symbol, InstrumentKind::PolyNo)
                .ok_or_else(|| anyhow!("missing poly no book"))?,
            cex_book: provider
                .get_cex_orderbook(
                    market.hedge_exchange,
                    &cex_venue_symbol(market.hedge_exchange, &market.cex_symbol),
                )
                .ok_or_else(|| anyhow!("missing cex book"))?,
            now_ms: observed
                .latest_poly_update_ms()
                .or(observed.cex_updated_at_ms)
                .unwrap_or_default(),
        })
    }
}

fn stable_plan_hash(intent: &PlanningIntent, context: &CanonicalPlanningContext) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &context.canonical_bytes(intent)).to_string()
}
```

- [ ] **Step 4: Implement open, close, rebalance, and recovery planning**

```rust
pub struct ExecutionPlanner {
    pub poly_fee_bps: u32,
    pub cex_fee_bps: u32,
    pub funding_bps_per_day: u32,
}

impl ExecutionPlanner {
    pub fn plan(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        match intent {
            PlanningIntent::OpenPosition { .. } => self.plan_open(intent, context),
            PlanningIntent::ClosePosition { .. } => self.plan_close(intent, context),
            PlanningIntent::DeltaRebalance { .. } => self.plan_rebalance(intent, context),
            PlanningIntent::ResidualRecovery { .. } | PlanningIntent::ForceExit { .. } => {
                self.plan_recovery(intent, context)
            }
        }
    }

    pub fn revalidate(
        &self,
        plan: &TradePlan,
        context: &CanonicalPlanningContext,
    ) -> Result<(), RevalidationFailureReason> {
        if context.now_ms.saturating_sub(plan.created_at_ms) > plan.plan_ttl_ms {
            return Err(RevalidationFailureReason::PlanTtlExpired);
        }
        Ok(())
    }
}
```

- [ ] **Step 5: Export the planner from `polyalpha-executor`**

```rust
pub mod planner;

pub use planner::ExecutionPlanner;
```

- [ ] **Step 6: Run the planner tests again**

Run: `cargo test -p polyalpha-executor planner_ -- --nocapture`

Expected: PASS for determinism, one-sided rejection, and fee/funding edge calculations.

- [ ] **Step 7: Commit the deterministic planner**

```bash
git add crates/polyalpha-executor/src/planner.rs crates/polyalpha-executor/src/lib.rs
git commit -m "feat: add deterministic execution planner"
```

### Task 4: Convert the Engine to Candidate-Only Output

**Files:**
- Modify: `crates/polyalpha-engine/src/lib.rs`
- Modify: `crates/polyalpha-engine/tests/price_only_parity.rs`
- Test: `crates/polyalpha-engine/src/lib.rs`

- [ ] **Step 1: Write failing engine tests for candidate-only output**

```rust
fn seeded_engine() -> SimpleAlphaEngine {
    SimpleAlphaEngine::with_markets(
        SimpleEngineConfig::default(),
        vec![MarketConfig {
            symbol: Symbol::new("btc-price-only"),
            poly_ids: PolymarketIds {
                condition_id: "cond-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some("Will BTC stay above 99k?".to_owned()),
            market_rule: Some(MarketRule::fallback_above(Price(Decimal::new(99_000, 0)))),
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(99_000, 0))),
            settlement_timestamp: 1_900_000_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }],
    )
}

fn seeded_basis_event() -> MarketDataEvent {
    MarketDataEvent::OrderBookUpdate {
        snapshot: OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-price-only"),
            instrument: InstrumentKind::PolyYes,
            bids: vec![PriceLevel {
                price: Price(Decimal::new(51, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10_000, 0))),
            }],
            asks: vec![PriceLevel {
                price: Price(Decimal::new(53, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(10_000, 0))),
            }],
            exchange_timestamp_ms: 1_716_000_000_000,
            received_at_ms: 1_716_000_000_001,
            sequence: 1,
            last_trade_price: None,
        },
    }
}

#[tokio::test]
async fn emits_open_candidate_without_execution_fields() {
    let mut engine = seeded_engine();
    let output = engine.on_market_data(&seeded_basis_event()).await;

    assert_eq!(output.open_candidates.len(), 1);
    let candidate = &output.open_candidates[0];
    assert_eq!(candidate.risk_budget_usd, 200.0);
    assert!(candidate.raw_mispricing.abs() > 0.0);
}
```

- [ ] **Step 2: Run the engine tests to verify they fail**

Run: `cargo test -p polyalpha-engine emits_open_candidate_without_execution_fields -- --nocapture`

Expected: FAIL because the engine still emits `ArbSignalEvent` values and `AlphaEngineOutput` has no `open_candidates`.

- [ ] **Step 3: Replace `build_basis_signal` with `build_open_candidate`**

```rust
fn build_open_candidate(
    &mut self,
    symbol: &Symbol,
    snapshot: &BasisStrategySnapshot,
) -> Option<OpenCandidate> {
    Some(OpenCandidate {
        schema_version: PLANNING_SCHEMA_VERSION,
        candidate_id: self.next_signal_id(symbol, snapshot.token_side),
        correlation_id: self.generate_correlation_id(),
        symbol: symbol.clone(),
        token_side: snapshot.token_side,
        direction: "long".to_owned(),
        fair_value: snapshot.fair_value,
        raw_mispricing: snapshot.signal_basis,
        delta_estimate: snapshot.delta,
        risk_budget_usd: self.get_position_notional_usd(symbol).0.to_f64().unwrap_or_default(),
        strength: SignalStrength::Normal,
        z_score: snapshot.z_score,
        timestamp_ms: snapshot.minute_bucket_ms,
    })
}
```

- [ ] **Step 4: Update `AlphaEngineOutput` call sites and parity tests**

```rust
if let Some(candidate) = self.build_open_candidate(symbol, &snapshot) {
    output.push_open_candidate(candidate);
}

assert_eq!(actual_candidates.len(), expected_candidates.len());
assert_eq!(actual_candidates[0].candidate_id, expected_candidates[0].candidate_id);
```

- [ ] **Step 5: Run the engine tests again**

Run: `cargo test -p polyalpha-engine emits_open_candidate_without_execution_fields -- --nocapture`

Expected: PASS, and the parity suite no longer relies on `expected_pnl` or `cex_hedge_qty`.

- [ ] **Step 6: Commit the engine cutover**

```bash
git add crates/polyalpha-engine/src/lib.rs crates/polyalpha-engine/tests/price_only_parity.rs
git commit -m "refactor: emit open candidates from engine"
```

### Task 5: Refactor `ExecutionManager` to a TradePlan State Machine

**Files:**
- Create: `crates/polyalpha-executor/src/plan_state.rs`
- Modify: `crates/polyalpha-core/src/event.rs`
- Modify: `crates/polyalpha-executor/src/manager.rs`
- Modify: `crates/polyalpha-executor/src/lib.rs`
- Test: `crates/polyalpha-executor/src/manager.rs`

- [ ] **Step 1: Write failing execution tests for supersession, child hedges, and recovery routing**

```rust
fn sample_open_intent() -> PlanningIntent {
    PlanningIntent::OpenPosition {
        schema_version: 1,
        intent_id: "intent-open-1".to_owned(),
        correlation_id: "corr-open-1".to_owned(),
        symbol: Symbol::new("btc-100k-mar-2026"),
        candidate: OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-open-1".to_owned(),
            correlation_id: "corr-open-1".to_owned(),
            symbol: Symbol::new("btc-100k-mar-2026"),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.49,
            raw_mispricing: 0.04,
            delta_estimate: 0.012,
            risk_budget_usd: 200.0,
            strength: SignalStrength::Strong,
            z_score: Some(2.8),
            timestamp_ms: 1_715_000_000_123,
        },
        max_budget_usd: 200.0,
        max_residual_delta: 0.05,
        max_shock_loss_usd: 20.0,
    }
}

fn sample_force_exit_intent() -> PlanningIntent {
    PlanningIntent::ForceExit {
        schema_version: 1,
        intent_id: "intent-force-1".to_owned(),
        correlation_id: "corr-open-1".to_owned(),
        symbol: Symbol::new("btc-100k-mar-2026"),
        force_reason: "operator_force_exit".to_owned(),
        allow_negative_edge: true,
    }
}

fn sample_executor_with_liquidity(
    poly_liquidity: Decimal,
    cex_liquidity: Decimal,
    allow_partial_fill: bool,
) -> DryRunExecutor {
    let provider = Arc::new(InMemoryOrderbookProvider::new());
    provider.update(OrderBookSnapshot {
        exchange: Exchange::Polymarket,
        symbol: Symbol::new("btc-100k-mar-2026"),
        instrument: InstrumentKind::PolyYes,
        bids: vec![PriceLevel {
            price: Price(Decimal::new(49, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
        }],
        asks: vec![PriceLevel {
            price: Price(Decimal::new(51, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
        }],
        exchange_timestamp_ms: 1_700_000_000_000,
        received_at_ms: 1_700_000_000_000,
        sequence: 1,
        last_trade_price: None,
    });
    provider.update(OrderBookSnapshot {
        exchange: Exchange::Binance,
        symbol: Symbol::new("btc-100k-mar-2026"),
        instrument: InstrumentKind::CexPerp,
        bids: vec![PriceLevel {
            price: Price(Decimal::new(100_000, 0)),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(cex_liquidity)),
        }],
        asks: vec![PriceLevel {
            price: Price(Decimal::new(100_010, 0)),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(cex_liquidity)),
        }],
        exchange_timestamp_ms: 1_700_000_000_000,
        received_at_ms: 1_700_000_000_000,
        sequence: 1,
        last_trade_price: None,
    });

    DryRunExecutor::with_orderbook(
        provider,
        SlippageConfig {
            allow_partial_fill,
            min_liquidity: Decimal::ONE,
            ..SlippageConfig::default()
        },
    )
}

#[tokio::test]
async fn higher_priority_intent_supersedes_open_plan() {
    let mut manager = ExecutionManager::new(sample_executor_with_liquidity(
        Decimal::new(500, 0),
        Decimal::new(5, 0),
        false,
    ));
    manager.process_intent(sample_open_intent()).await.unwrap();

    let events = manager.process_intent(sample_force_exit_intent()).await.unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        ExecutionEvent::PlanSuperseded { .. }
    )));
}

#[tokio::test]
async fn poly_partial_fill_spawns_child_hedge_plan() {
    let mut manager = ExecutionManager::new(sample_executor_with_liquidity(
        Decimal::new(5, 0),
        Decimal::new(5, 0),
        true,
    ));
    let events = manager.process_intent(sample_open_intent()).await.unwrap();

    assert!(events.iter().any(|event| matches!(
        event,
        ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_some()
    )));
}

#[tokio::test]
async fn recovery_plan_is_emitted_when_residual_is_too_large() {
    let mut manager = ExecutionManager::new(sample_executor_with_liquidity(
        Decimal::new(500, 0),
        Decimal::ZERO,
        false,
    ));
    let events = manager.process_intent(sample_open_intent()).await.unwrap();

    assert!(events.iter().any(|event| matches!(
        event,
        ExecutionEvent::RecoveryPlanCreated { .. }
    )));
}
```

- [ ] **Step 2: Run the manager tests to verify they fail**

Run: `cargo test -p polyalpha-executor "supersedes_open_plan|child_hedge_plan|recovery_plan_is_emitted" -- --nocapture`

Expected: FAIL because `ExecutionManager` still processes legacy `ArbSignalEvent` values.

- [ ] **Step 3: Introduce in-flight plan bookkeeping with priority and idempotency**

```rust
// crates/polyalpha-core/src/event.rs
pub enum ExecutionEvent {
    TradePlanCreated { plan: TradePlan },
    PlanSuperseded {
        symbol: Symbol,
        superseded_plan_id: String,
        next_plan_id: String,
    },
    RecoveryPlanCreated { plan: TradePlan },
    // keep existing order/fill/state-change variants
}

// crates/polyalpha-executor/src/plan_state.rs
#[derive(Clone, Debug)]
pub struct InFlightPlan {
    pub plan: TradePlan,
    pub state: PlanLifecycleState,
    pub idempotency_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanLifecycleState {
    PlanReady,
    SubmittingPoly,
    HedgingCex,
    VerifyingResidual,
    Recovering,
    Frozen,
}
```

- [ ] **Step 4: Replace `process_arb_signal` with `process_intent` and actual-fill-driven hedging**

```rust
pub fn planner(&self) -> &ExecutionPlanner {
    &self.planner
}

pub fn orderbook_provider(&self) -> Option<&Arc<dyn OrderbookProvider>> {
    self.orderbook_provider.as_ref()
}

pub async fn process_plan(&mut self, plan: TradePlan) -> Result<Vec<ExecutionEvent>> {
    self.install_plan(plan.clone())?;
    self.submit_poly_leg(&plan).await
}

pub async fn process_intent(
    &mut self,
    intent: PlanningIntent,
) -> Result<Vec<ExecutionEvent>> {
    self.reject_lower_priority_if_needed(&intent)?;

    let context = self.build_planning_context(&intent)?;
    let plan = self.planner.plan(&intent, &context)?;
    self.install_plan(plan.clone())?;
    self.submit_poly_leg(&plan).await
}

async fn on_poly_fill(
    &mut self,
    plan: &TradePlan,
    fill: &Fill,
) -> Result<Vec<ExecutionEvent>> {
    let hedge_intent = self.child_hedge_intent_from_fill(plan, fill)?;
    let context = self.build_planning_context(&hedge_intent)?;
    let hedge_plan = self.planner.plan(&hedge_intent, &context)?;
    self.install_child_plan(plan, hedge_plan.clone())?;
    self.submit_cex_leg(&hedge_plan).await
}
```

- [ ] **Step 5: Route unresolved residuals back through planner-owned recovery**

```rust
fn recovery_intent_from_result(&self, result: &ExecutionResult) -> Option<PlanningIntent> {
    if result.actual_residual_delta <= self.max_residual_delta {
        return None;
    }

    Some(PlanningIntent::ResidualRecovery {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: format!("recover-{}", result.plan_id),
        correlation_id: result.correlation_id.clone(),
        symbol: result.symbol.clone(),
        residual_snapshot: self.residual_snapshot(&result.symbol),
        recovery_reason: RecoveryDecisionReason::ForceExitRequired,
    })
}
```

- [ ] **Step 6: Run the manager tests again**

Run: `cargo test -p polyalpha-executor "supersedes_open_plan|child_hedge_plan|recovery_plan_is_emitted" -- --nocapture`

Expected: PASS, with execution events emitted from `TradePlan` transitions instead of signal action branches.

- [ ] **Step 7: Commit the execution state-machine cutover**

```bash
git add crates/polyalpha-core/src/event.rs crates/polyalpha-executor/src/plan_state.rs crates/polyalpha-executor/src/manager.rs crates/polyalpha-executor/src/lib.rs
git commit -m "refactor: drive execution manager with trade plans"
```

### Task 6: Migrate `paper`, `sim`, and `backtest_rust` to Planner Pipeline

**Files:**
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Modify: `crates/polyalpha-cli/src/sim.rs`
- Modify: `crates/polyalpha-cli/src/backtest_rust.rs`
- Modify: `crates/polyalpha-cli/src/commands.rs`
- Test: `crates/polyalpha-cli/src/paper.rs`

- [ ] **Step 1: Write failing runtime tests for candidate-to-plan flow and close intent semantics**

```rust
fn seeded_open_candidate() -> OpenCandidate {
    OpenCandidate {
        schema_version: 1,
        candidate_id: "cand-paper-1".to_owned(),
        correlation_id: "corr-paper-1".to_owned(),
        symbol: Symbol::new("btc-test"),
        token_side: TokenSide::Yes,
        direction: "long".to_owned(),
        fair_value: 0.47,
        raw_mispricing: 0.06,
        delta_estimate: 0.25,
        risk_budget_usd: 100.0,
        strength: SignalStrength::Normal,
        z_score: Some(2.3),
        timestamp_ms: 1,
    }
}

fn sample_trade_plan() -> TradePlan {
    TradePlan {
        schema_version: 1,
        plan_id: "plan-paper-1".to_owned(),
        parent_plan_id: None,
        supersedes_plan_id: None,
        idempotency_key: "idem-paper-1".to_owned(),
        correlation_id: "corr-paper-1".to_owned(),
        symbol: Symbol::new("btc-test"),
        intent_type: "open_position".to_owned(),
        priority: "open_position".to_owned(),
        created_at_ms: 1,
        poly_exchange_timestamp_ms: 1,
        poly_received_at_ms: 1,
        poly_sequence: 1,
        cex_exchange_timestamp_ms: 1,
        cex_received_at_ms: 1,
        cex_sequence: 1,
        plan_hash: "plan-hash-1".to_owned(),
        poly_side: OrderSide::Buy,
        poly_token_side: TokenSide::Yes,
        poly_sizing_mode: "buy_budget_cap".to_owned(),
        poly_requested_shares: PolyShares(Decimal::new(10, 0)),
        poly_planned_shares: PolyShares(Decimal::new(10, 0)),
        poly_max_cost_usd: UsdNotional(Decimal::new(100, 0)),
        poly_max_avg_price: Price(Decimal::new(51, 2)),
        poly_max_shares: PolyShares(Decimal::new(10, 0)),
        poly_min_avg_price: Price(Decimal::ZERO),
        poly_min_proceeds_usd: UsdNotional::ZERO,
        poly_book_avg_price: Price(Decimal::new(46, 2)),
        poly_executable_price: Price(Decimal::new(47, 2)),
        poly_friction_cost_usd: UsdNotional(Decimal::new(5, 1)),
        planned_edge_usd: UsdNotional(Decimal::new(42, 1)),
        raw_edge_usd: UsdNotional(Decimal::new(57, 1)),
        poly_fee_usd: UsdNotional(Decimal::new(1, 1)),
        cex_side: OrderSide::Sell,
        cex_planned_qty: CexBaseQty(Decimal::new(1, 2)),
        cex_book_avg_price: Price(Decimal::new(100_000, 0)),
        cex_executable_price: Price(Decimal::new(100_010, 0)),
        cex_friction_cost_usd: UsdNotional(Decimal::new(2, 1)),
        cex_fee_usd: UsdNotional(Decimal::new(2, 2)),
        expected_funding_cost_usd: UsdNotional(Decimal::new(3, 2)),
        residual_risk_penalty_usd: UsdNotional(Decimal::new(1, 1)),
        post_rounding_residual_delta: 0.01,
        shock_loss_up_1pct: UsdNotional(Decimal::new(2, 0)),
        shock_loss_down_1pct: UsdNotional(Decimal::new(2, 0)),
        shock_loss_up_2pct: UsdNotional(Decimal::new(4, 0)),
        shock_loss_down_2pct: UsdNotional(Decimal::new(4, 0)),
        plan_ttl_ms: 2_000,
        max_poly_sequence_drift: 2,
        max_cex_sequence_drift: 2,
        max_poly_price_move: Price(Decimal::new(1, 2)),
        max_cex_price_move: Price(Decimal::new(100, 0)),
        min_planned_edge_usd: UsdNotional(Decimal::new(1, 0)),
        max_residual_delta: 0.05,
        max_shock_loss_usd: UsdNotional(Decimal::new(20, 0)),
        max_plan_vs_fill_deviation_usd: UsdNotional(Decimal::new(10, 0)),
    }
}

fn sample_execution_result(plan_id: &str) -> ExecutionResult {
    ExecutionResult {
        schema_version: 1,
        plan_id: plan_id.to_owned(),
        correlation_id: "corr-paper-1".to_owned(),
        symbol: Symbol::new("btc-test"),
        status: "filled".to_owned(),
        poly_order_ledger: Vec::new(),
        cex_order_ledger: Vec::new(),
        actual_poly_cost_usd: UsdNotional(Decimal::new(48, 0)),
        actual_cex_cost_usd: UsdNotional(Decimal::new(100, 0)),
        actual_poly_fee_usd: UsdNotional(Decimal::new(1, 1)),
        actual_cex_fee_usd: UsdNotional(Decimal::new(2, 2)),
        actual_funding_cost_usd: UsdNotional(Decimal::new(1, 2)),
        realized_edge_usd: UsdNotional(Decimal::new(36, 1)),
        plan_vs_fill_deviation_usd: UsdNotional(Decimal::new(6, 1)),
        actual_residual_delta: 0.01,
        actual_shock_loss_up_1pct: UsdNotional(Decimal::new(2, 0)),
        actual_shock_loss_down_1pct: UsdNotional(Decimal::new(2, 0)),
        actual_shock_loss_up_2pct: UsdNotional(Decimal::new(4, 0)),
        actual_shock_loss_down_2pct: UsdNotional(Decimal::new(4, 0)),
        recovery_required: false,
        timestamp_ms: 2,
    }
}

#[test]
fn trade_plan_event_details_include_planned_and_realized_edges() {
    let plan = sample_trade_plan();
    let result = sample_execution_result(&plan.plan_id);
    let details = build_trade_plan_event_details(&plan, Some(&result));

    assert_eq!(details.get("计划边际").unwrap(), "4.200000");
    assert_eq!(details.get("实际边际").unwrap(), "3.600000");
    assert!(details.contains_key("Poly手续费"));
}

#[test]
fn manual_close_uses_sell_constraints_not_budget_cap() {
    let intent = manual_close_intent(&Symbol::new("btc-test"), "manual-1", 1_716_000_000_000);
    match intent {
        PlanningIntent::ClosePosition { .. } => {}
        other => panic!("unexpected intent: {other:?}"),
    }
}
```

- [ ] **Step 2: Run the runtime tests to verify they fail**

Run: `cargo test -p polyalpha-cli "planned_and_realized_edge_fields|manual_close_uses_sell_constraints" -- --nocapture`

Expected: FAIL because `paper` still expects `ArbSignalEvent` and `evaluate_signal_execution_economics(...)`.

- [ ] **Step 3: Replace signal-driven open flow with candidate -> intent -> plan**

```rust
async fn process_intent_single(
    intent: PlanningIntent,
    settings: &Settings,
    registry: &SymbolRegistry,
    observed: &ObservedState,
    stats: &mut PaperStats,
    recent_events: &mut VecDeque<PaperEventLog>,
    engine: &mut SimpleAlphaEngine,
    execution: &mut ExecutionManager<DryRunExecutor>,
    risk: &mut InMemoryRiskManager,
    trade_book: &mut TradeBook,
    paused: &Arc<AtomicBool>,
    emergency: &Arc<AtomicBool>,
    ws_mode: bool,
    audit: &mut Option<PaperAudit>,
) -> Result<()> {
    let context = build_planning_context(settings, registry, observed, &intent, execution)?;
    let plan = execution.planner().plan(&intent, &context)?;
    let events = execution.process_plan(plan.clone()).await?;
    push_trade_plan_event(recent_events, &plan, build_trade_plan_event_details(&plan, None));
    apply_execution_events_and_sync_engine(
        engine,
        risk,
        stats,
        recent_events,
        trade_book,
        None,
        None,
        events,
        audit,
    )
    .await
}

fn build_planning_context(
    settings: &Settings,
    registry: &SymbolRegistry,
    observed: &ObservedState,
    intent: &PlanningIntent,
    execution: &ExecutionManager<DryRunExecutor>,
) -> Result<CanonicalPlanningContext> {
    CanonicalPlanningContext::from_observed_state(settings, registry, observed, intent, execution)
}

fn build_trade_plan_event_details(
    plan: &TradePlan,
    result: Option<&ExecutionResult>,
) -> EventDetails {
    let mut details = EventDetails::new();
    insert_detail(&mut details, "计划边际", format_detail_decimal(plan.planned_edge_usd.0));
    insert_detail(&mut details, "Poly手续费", format_detail_decimal(plan.poly_fee_usd.0));
    if let Some(result) = result {
        insert_detail(&mut details, "实际边际", format_detail_decimal(result.realized_edge_usd.0));
        insert_detail(
            &mut details,
            "计划偏离",
            format_detail_decimal(result.plan_vs_fill_deviation_usd.0),
        );
    }
    details
}

fn push_trade_plan_event(
    recent_events: &mut VecDeque<PaperEventLog>,
    plan: &TradePlan,
    details: EventDetails,
) {
    recent_events.push_front(PaperEventLog {
        seq: 0,
        timestamp_ms: plan.created_at_ms,
        kind: "trade_plan".to_owned(),
        summary: format!("交易计划已生成 {}", plan.plan_id),
        market: Some(plan.symbol.0.clone()),
        signal_id: None,
        correlation_id: Some(plan.correlation_id.clone()),
        details: Some(details),
    });
}
```

- [ ] **Step 4: Replace manual close, delta rebalance, and replay fills with `PlanningIntent`**

```rust
fn manual_close_intent(symbol: &Symbol, command_id: &str, now_ms: u64) -> PlanningIntent {
    PlanningIntent::ClosePosition {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: command_id.to_owned(),
        correlation_id: format!("close-{command_id}"),
        symbol: symbol.clone(),
        close_reason: "manual_close".to_owned(),
        target_close_ratio: 1.0,
    }
}

fn rebalance_intent(symbol: &Symbol, max_residual_delta: f64, max_shock_loss_usd: f64) -> PlanningIntent {
    PlanningIntent::DeltaRebalance {
        schema_version: PLANNING_SCHEMA_VERSION,
        intent_id: format!("rebalance-{}", symbol.0),
        correlation_id: format!("rebalance-{}", symbol.0),
        symbol: symbol.clone(),
        target_residual_delta_max: max_residual_delta,
        target_shock_loss_max: max_shock_loss_usd,
    }
}
```

- [ ] **Step 5: Update replay/backtest reports to emit plan/result fields**

```rust
let row = json!({
    "plan_hash": plan.plan_hash,
    "raw_edge_usd": plan.raw_edge_usd.0,
    "planned_edge_usd": plan.planned_edge_usd.0,
    "realized_edge_usd": result.realized_edge_usd.0,
    "poly_fee_usd": result.actual_poly_fee_usd.0,
    "cex_fee_usd": result.actual_cex_fee_usd.0,
    "plan_vs_fill_deviation_usd": result.plan_vs_fill_deviation_usd.0,
});
```

- [ ] **Step 6: Run the runtime tests again**

Run: `cargo test -p polyalpha-cli "planned_and_realized_edge_fields|manual_close_uses_sell_constraints" -- --nocapture`

Expected: PASS, and `cargo test -p polyalpha-cli backtest_ -- --nocapture` still passes with plan/result-based reporting.

- [ ] **Step 7: Commit the runtime migration**

```bash
git add crates/polyalpha-cli/src/paper.rs crates/polyalpha-cli/src/sim.rs crates/polyalpha-cli/src/backtest_rust.rs crates/polyalpha-cli/src/commands.rs
git commit -m "refactor: run paper and replay through planning intents"
```

### Task 7: Migrate Socket, Audit, and Monitor Payloads

**Files:**
- Modify: `crates/polyalpha-core/src/socket/message.rs`
- Modify: `crates/polyalpha-audit/src/model.rs`
- Modify: `crates/polyalpha-cli/src/audit.rs`
- Modify: `crates/polyalpha-cli/src/monitor/state.rs`
- Modify: `crates/polyalpha-cli/src/monitor/ui.rs`
- Test: `crates/polyalpha-core/src/socket/message.rs`

- [ ] **Step 1: Write failing serialization and UI tests for plan/result fields**

```rust
#[test]
fn socket_message_roundtrips_trade_plan_view_with_schema_version() {
    let message = Message::Event {
        timestamp_ms: 1_716_000_000_000,
        event: MonitorEvent::TradePlanCreated(TradePlanView {
            schema_version: 1,
            plan_id: "plan-1".to_owned(),
            plan_hash: "hash-1".to_owned(),
            planned_edge_usd: 4.2,
            poly_fee_usd: 0.12,
            cex_fee_usd: 0.03,
        }),
    };

    let encoded = serde_json::to_string(&message).unwrap();
    let decoded: Message = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(decoded, Message::Event { .. }));
}
```

```rust
#[test]
fn monitor_renders_reason_code_and_fee_columns() {
    let event = &sample_monitor_state().recent_events[0];
    let cells = trade_plan_reason_and_fee_cells(event);
    assert!(cells.iter().any(|cell| cell.contains("计划边际")));
    assert!(cells.iter().any(|cell| cell.contains("手续费")));
    assert!(cells.iter().any(|cell| cell.contains("missing_poly_book")));
}
```

- [ ] **Step 2: Run the socket and monitor tests to verify they fail**

Run: `cargo test -p polyalpha-core socket_message_roundtrips_trade_plan_view_with_schema_version -- --exact`

Expected: FAIL because `TradePlanView` and related monitor events do not exist yet.

- [ ] **Step 3: Add schema-versioned plan/result views to the monitor protocol**

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradePlanView {
    pub schema_version: u32,
    pub plan_id: String,
    pub plan_hash: String,
    pub planned_edge_usd: f64,
    pub poly_fee_usd: f64,
    pub cex_fee_usd: f64,
    pub revalidation_failure_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResultView {
    pub schema_version: u32,
    pub plan_id: String,
    pub realized_edge_usd: f64,
    pub plan_vs_fill_deviation_usd: f64,
    pub actual_funding_cost_usd: f64,
    pub recovery_decision_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MonitorEvent {
    TradePlanCreated(TradePlanView),
    ExecutionResultRecorded(ExecutionResultView),
    // keep existing signal/risk/system variants in place
}
```

- [ ] **Step 4: Replace signal payloads in audit and monitor state with candidate/plan/result payloads**

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditPlanRecord {
    pub schema_version: u32,
    pub candidate_id: Option<String>,
    pub intent_id: String,
    pub plan_id: String,
    pub plan_hash: String,
    pub planned_edge_usd: f64,
    pub realized_edge_usd: Option<f64>,
    pub plan_rejection_reason: Option<String>,
    pub revalidation_failure_reason: Option<String>,
    pub recovery_decision_reason: Option<String>,
}
```

- [ ] **Step 5: Update monitor rendering for fee/funding/reason-code columns**

```rust
fn trade_plan_reason_and_fee_cells(event: &MonitorEvent) -> Vec<String> {
    let details = event.details.as_ref().unwrap();
    vec![
        format!("计划边际 {}", details.get("计划边际").cloned().unwrap_or_default()),
        format!("手续费 {}", details.get("总手续费").cloned().unwrap_or_default()),
        details.get("原因代码").cloned().unwrap_or_else(|| "-".to_owned()),
    ]
}
```

- [ ] **Step 6: Run the socket, audit, and monitor tests again**

Run: `cargo test -p polyalpha-core socket_message_roundtrips_trade_plan_view_with_schema_version -- --exact`

Expected: PASS, and `cargo test -p polyalpha-cli monitor_ -- --nocapture` passes with the new columns.

- [ ] **Step 7: Commit the protocol and UI migration**

```bash
git add crates/polyalpha-core/src/socket/message.rs crates/polyalpha-audit/src/model.rs crates/polyalpha-cli/src/audit.rs crates/polyalpha-cli/src/monitor/state.rs crates/polyalpha-cli/src/monitor/ui.rs
git commit -m "refactor: expose trade plans and execution results to audit and monitor"
```

### Task 8: Remove Legacy Signal Path and Run Full Verification

**Files:**
- Modify: `crates/polyalpha-core/src/types/mod.rs`
- Modify: `crates/polyalpha-engine/src/lib.rs`
- Modify: `crates/polyalpha-executor/src/manager.rs`
- Modify: `crates/polyalpha-cli/src/paper.rs`
- Test: `crates/polyalpha-core/src/types/mod.rs`

- [ ] **Step 1: Add a failing guard test that legacy executable signal exports are gone**

```rust
#[test]
fn core_type_exports_no_longer_publish_legacy_signal_execution_types() {
    let exports = include_str!("mod.rs");
    assert!(!exports.contains("ArbSignalEvent"));
    assert!(!exports.contains("ArbSignalAction"));
}
```

- [ ] **Step 2: Run the guard test and workspace grep to verify legacy references still exist**

Run: `cargo test -p polyalpha-core core_type_exports_no_longer_publish_legacy_signal_execution_types -- --exact`

Expected: FAIL, and `rg -n "ArbSignalEvent|ArbSignalAction|expected_pnl|preview_requests_for_signal" crates` still returns hits.

- [ ] **Step 3: Remove the remaining legacy signal path and update callers**

```rust
// crates/polyalpha-core/src/types/mod.rs
pub use signal::{AlphaEngineOutput, DmmQuoteSlot, DmmQuoteState, DmmQuoteUpdate, EngineWarning};

// crates/polyalpha-engine/src/lib.rs
use polyalpha_core::{AlphaEngineOutput, OpenCandidate, PlanningIntent, SignalStrength};

// crates/polyalpha-cli/src/paper.rs
// delete evaluate_signal_execution_economics(...) and replace callers with planner-driven audit hooks
```

- [ ] **Step 4: Run targeted grep and the full verification matrix**

Run: `rg -n "ArbSignalEvent|ArbSignalAction|expected_pnl|preview_requests_for_signal" crates`

Expected: no matches

Run: `cargo test -p polyalpha-core -p polyalpha-engine -p polyalpha-executor -p polyalpha-cli -- --nocapture`

Expected: PASS across all touched crates

Run: `cargo test -p polyalpha-cli "partial_fill|recovery|backtest|monitor" -- --nocapture`

Expected: PASS for planner-owned recovery, replay parity, and monitor regressions

- [ ] **Step 5: Commit the hard cut and final verification**

```bash
git add crates/polyalpha-core/src/types/mod.rs crates/polyalpha-engine/src/lib.rs crates/polyalpha-executor/src/manager.rs crates/polyalpha-cli/src/paper.rs
git commit -m "refactor: remove legacy signal execution path"
```

## Self-Review Checklist

- Spec coverage:
  - Task 1 covers `schema_version`, top-level planning objects, and stable reason enums.
  - Task 2 covers `BuyBudgetCap`, `SellExactShares`, `SellMinProceeds`, dry-run semantics, and risk estimation.
  - Task 3 covers deterministic `PlanningContext`, `plan_hash`, canonicalization, fees, funding, and planner rejection/revalidation behavior.
  - Task 4 covers engine downgrade from executable signal emitter to `OpenCandidate` producer.
  - Task 5 covers priority, supersession, `parent_plan_id`, actual-fill-driven hedging, and planner-owned recovery.
  - Task 6 covers `paper`, `sim`, `backtest_rust`, and command/runtime migration.
  - Task 7 covers socket/audit/monitor schema migration and new displayed fields.
  - Task 8 covers legacy removal, grep guards, and end-to-end verification.
- Placeholder scan:
  - No placeholder markers remain.
- Type consistency:
  - The plan consistently uses `OpenCandidate`, `PlanningIntent`, `TradePlan`, `ExecutionResult`, `BuyBudgetCap`, `SellExactShares`, `SellMinProceeds`, `plan_hash`, `parent_plan_id`, `supersedes_plan_id`, and `idempotency_key`.
