use std::{collections::BTreeMap, sync::Arc};

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use polyalpha_core::{
    CexBaseQty, ExecutionCostConfig, ExecutionResult, Fill, FundingCostMode, InstrumentKind,
    OpenCandidate, OrderBookSnapshot, OrderRequest, OrderSide, PlanRejectionReason, PlanningIntent,
    PolyOrderRequest, PolyShares, PolySizingInstruction, Price, RecoveryDecisionReason,
    ResidualSnapshot, RevalidationFailureReason, Symbol, TokenSide, TradePlan, UsdNotional,
    VenueQuantity, PLANNING_SCHEMA_VERSION,
};

use crate::{
    dry_run::{DryRunExecutor, OrderExecutionEstimate, SlippageConfig},
    orderbook_provider::InMemoryOrderbookProvider,
};

const DEFAULT_PLAN_TTL_MS: u64 = 1_000;
const DEFAULT_MAX_SEQUENCE_DRIFT: u64 = 2;
const DEFAULT_POLY_SLIPPAGE_BPS: u64 = 50;
const DEFAULT_CEX_SLIPPAGE_BPS: u64 = 2;
const OPEN_INSTANT_LOSS_BINARY_SEARCH_STEPS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanRejection {
    pub reason: PlanRejectionReason,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalPlanningContext {
    pub planner_depth_levels: usize,
    pub poly_yes_book: OrderBookSnapshot,
    pub poly_no_book: OrderBookSnapshot,
    pub cex_book: OrderBookSnapshot,
    pub cex_qty_step: Decimal,
    pub current_poly_yes_shares: Decimal,
    pub current_poly_no_shares: Decimal,
    pub current_cex_net_qty: Decimal,
    pub now_ms: u64,
}

#[derive(Clone, Debug)]
struct OpenSizingSelection {
    budget_cap_usd: Decimal,
    capped_max_shares: PolyShares,
    poly_estimate: OrderExecutionEstimate,
    poly_planned_shares: PolyShares,
    cex_planned_qty: CexBaseQty,
    post_rounding_residual_qty: Decimal,
    cex_estimate: OrderExecutionEstimate,
}

#[derive(Clone, Debug)]
enum OpenSizingProbe {
    Candidate(OpenSizingSelection),
    BelowExecutableFloor,
    Rejected,
}

impl CanonicalPlanningContext {
    pub fn canonical_bytes(&self, intent: &PlanningIntent) -> Vec<u8> {
        let canonical = self.clone().canonicalize();
        serde_json::to_vec(&(intent, canonical)).expect("canonical planning context")
    }

    pub fn canonicalize(mut self) -> Self {
        self.poly_yes_book = canonicalize_book(&self.poly_yes_book, self.planner_depth_levels);
        self.poly_no_book = canonicalize_book(&self.poly_no_book, self.planner_depth_levels);
        self.cex_book = canonicalize_book(&self.cex_book, self.planner_depth_levels);
        self
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionPlanner {
    pub poly_fee_bps: u32,
    pub cex_taker_fee_bps: u32,
    pub cex_maker_fee_bps: u32,
    pub funding_mode: FundingCostMode,
    pub fallback_funding_bps_per_day: u32,
    pub poly_slippage_bps: u32,
    pub cex_slippage_bps: u32,
    pub max_open_instant_loss_pct_of_budget: Decimal,
}

impl Default for ExecutionPlanner {
    fn default() -> Self {
        Self {
            poly_fee_bps: 20,
            cex_taker_fee_bps: 5,
            cex_maker_fee_bps: 5,
            funding_mode: FundingCostMode::FallbackBps,
            fallback_funding_bps_per_day: 1,
            poly_slippage_bps: DEFAULT_POLY_SLIPPAGE_BPS as u32,
            cex_slippage_bps: DEFAULT_CEX_SLIPPAGE_BPS as u32,
            max_open_instant_loss_pct_of_budget: Decimal::new(4, 2),
        }
    }
}

fn recovery_reason_from_intent(intent: &PlanningIntent) -> Option<RecoveryDecisionReason> {
    match intent {
        PlanningIntent::ResidualRecovery {
            recovery_reason, ..
        } => Some(recovery_reason.clone()),
        _ => None,
    }
}

impl ExecutionPlanner {
    pub fn from_execution_costs(config: &ExecutionCostConfig) -> Self {
        Self {
            poly_fee_bps: config.poly_fee_bps,
            cex_taker_fee_bps: config.cex_taker_fee_bps,
            cex_maker_fee_bps: config.cex_maker_fee_bps,
            funding_mode: config.funding_mode,
            fallback_funding_bps_per_day: config.fallback_funding_bps_per_day,
            ..Self::default()
        }
    }

    pub fn cex_fee_bps(&self, is_maker: bool) -> u32 {
        if is_maker {
            self.cex_maker_fee_bps
        } else {
            self.cex_taker_fee_bps
        }
    }

    pub fn funding_bps_per_day(&self) -> u32 {
        match self.funding_mode {
            FundingCostMode::FallbackBps | FundingCostMode::ObservedRate => {
                self.fallback_funding_bps_per_day
            }
        }
    }

    pub fn plan(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        let context = context.clone().canonicalize();
        self.validate_context(&context)?;
        match intent {
            PlanningIntent::OpenPosition { .. } => self.plan_open(intent, &context),
            PlanningIntent::ClosePosition { .. } => {
                self.plan_flatten(intent, &context, "close_position", "sell_exact_shares")
            }
            PlanningIntent::DeltaRebalance { .. } => self.plan_delta_rebalance(intent, &context),
            PlanningIntent::ResidualRecovery { .. } => {
                self.plan_residual_recovery(intent, &context)
            }
            PlanningIntent::ForceExit { .. } => {
                self.plan_flatten(intent, &context, "force_exit", "sell_exact_shares")
            }
        }
    }

    pub fn revalidate(
        &self,
        plan: &TradePlan,
        context: &CanonicalPlanningContext,
    ) -> Result<(), RevalidationFailureReason> {
        let context = context.clone().canonicalize();
        if context.poly_yes_book.bids.is_empty()
            || context.poly_yes_book.asks.is_empty()
            || context.poly_no_book.bids.is_empty()
            || context.poly_no_book.asks.is_empty()
            || context.cex_book.bids.is_empty()
            || context.cex_book.asks.is_empty()
        {
            return Err(RevalidationFailureReason::BookMissingOnRevalidate);
        }
        if context.now_ms.saturating_sub(plan.created_at_ms) > plan.plan_ttl_ms {
            return Err(RevalidationFailureReason::PlanTtlExpired);
        }
        if context
            .poly_yes_book
            .sequence
            .saturating_sub(plan.poly_sequence)
            > plan.max_poly_sequence_drift
        {
            return Err(RevalidationFailureReason::PolySequenceDriftExceeded);
        }
        if context.cex_book.sequence.saturating_sub(plan.cex_sequence) > plan.max_cex_sequence_drift
        {
            return Err(RevalidationFailureReason::CexSequenceDriftExceeded);
        }
        let current_poly_price = match plan.poly_side {
            OrderSide::Buy => best_level_price(
                selected_poly_book(&context, plan.poly_token_side),
                OrderSide::Buy,
            ),
            OrderSide::Sell => best_level_price(
                selected_poly_book(&context, plan.poly_token_side),
                OrderSide::Sell,
            ),
        };
        if decimal_abs(current_poly_price.0 - plan.poly_executable_price.0)
            > plan.max_poly_price_move.0
        {
            return Err(RevalidationFailureReason::PolyPriceMoveExceeded);
        }
        let current_cex_price = best_level_price(&context.cex_book, plan.cex_side);
        if decimal_abs(current_cex_price.0 - plan.cex_executable_price.0)
            > plan.max_cex_price_move.0
        {
            return Err(RevalidationFailureReason::CexPriceMoveExceeded);
        }
        let (current_edge, shock_losses) = self
            .current_revalidation_metrics(plan, &context)
            .ok_or(RevalidationFailureReason::PlannedEdgeDeteriorated)?;
        if current_edge.0 < plan.min_planned_edge_usd.0 {
            return Err(RevalidationFailureReason::PlannedEdgeDeteriorated);
        }
        if shock_losses
            .iter()
            .any(|shock_loss| shock_loss.0 > plan.max_shock_loss_usd.0)
        {
            return Err(RevalidationFailureReason::ResidualRiskDeteriorated);
        }
        Ok(())
    }

    pub fn recovery_intent_from_result(
        &self,
        plan: &TradePlan,
        result: &ExecutionResult,
        context: &CanonicalPlanningContext,
    ) -> Option<PlanningIntent> {
        let context = context.clone().canonicalize();
        if result.actual_residual_delta <= plan.max_residual_delta {
            return None;
        }

        let residual_qty = CexBaseQty(
            Decimal::from_f64(result.actual_residual_delta)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO),
        );
        let normalized_qty = residual_qty.floor_to_step(context.cex_qty_step);
        let cex_top_up_viable = normalized_qty.0 > Decimal::ZERO
            && matches!(
                self.estimate_cex_order(&context, plan.cex_side, normalized_qty)
                    .status,
                polyalpha_core::OrderStatus::Filled
            );
        let flatten_plan = self
            .plan_flatten(
                &PlanningIntent::ResidualRecovery {
                    schema_version: PLANNING_SCHEMA_VERSION,
                    intent_id: format!("recover-{}", result.plan_id),
                    correlation_id: result.correlation_id.clone(),
                    symbol: result.symbol.clone(),
                    residual_snapshot: ResidualSnapshot {
                        schema_version: PLANNING_SCHEMA_VERSION,
                        residual_delta: result.actual_residual_delta,
                        planned_cex_qty: plan.cex_planned_qty,
                        current_poly_yes_shares: PolyShares(
                            context.current_poly_yes_shares.max(Decimal::ZERO),
                        ),
                        current_poly_no_shares: PolyShares(
                            context.current_poly_no_shares.max(Decimal::ZERO),
                        ),
                        preferred_cex_side: plan.cex_side,
                    },
                    recovery_reason: RecoveryDecisionReason::PolyFlattenOnlySafeRoute,
                },
                &context,
                "residual_recovery",
                "sell_exact_shares",
            )
            .ok();
        let flatten_cost = flatten_plan
            .as_ref()
            .map(|candidate| {
                candidate.poly_friction_cost_usd.0
                    + candidate.cex_friction_cost_usd.0
                    + candidate.poly_fee_usd.0
                    + candidate.cex_fee_usd.0
            })
            .unwrap_or(Decimal::MAX);
        let top_up_cost = if cex_top_up_viable {
            let estimate = self.estimate_cex_order(&context, plan.cex_side, normalized_qty);
            estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO).0
                + estimate.fee_usd.unwrap_or(UsdNotional::ZERO).0
                + bps_fee(estimate.executable_notional_usd, self.funding_bps_per_day()).0
        } else {
            Decimal::MAX
        };
        let recovery_reason = if top_up_cost < flatten_cost {
            RecoveryDecisionReason::CexTopUpCheaper
        } else if cex_top_up_viable {
            RecoveryDecisionReason::PolyFlattenCheaper
        } else if flatten_plan.is_some() {
            RecoveryDecisionReason::PolyFlattenOnlySafeRoute
        } else {
            RecoveryDecisionReason::ForceExitRequired
        };

        Some(PlanningIntent::ResidualRecovery {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: format!("recover-{}", result.plan_id),
            correlation_id: result.correlation_id.clone(),
            symbol: result.symbol.clone(),
            residual_snapshot: ResidualSnapshot {
                schema_version: PLANNING_SCHEMA_VERSION,
                residual_delta: result.actual_residual_delta,
                planned_cex_qty: plan.cex_planned_qty,
                current_poly_yes_shares: PolyShares(
                    context.current_poly_yes_shares.max(Decimal::ZERO),
                ),
                current_poly_no_shares: PolyShares(
                    context.current_poly_no_shares.max(Decimal::ZERO),
                ),
                preferred_cex_side: plan.cex_side,
            },
            recovery_reason,
        })
    }

    pub fn replan_from_execution(
        &self,
        parent_plan: &TradePlan,
        actual_fills: &[Fill],
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        let context = context.clone().canonicalize();
        let actual_poly_shares: Decimal = actual_fills
            .iter()
            .filter_map(|fill| match fill.quantity {
                VenueQuantity::PolyShares(shares) => Some(shares.0),
                VenueQuantity::CexBaseQty(_) => None,
            })
            .sum();
        if actual_poly_shares <= Decimal::ZERO || parent_plan.poly_planned_shares.0 <= Decimal::ZERO
        {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ResidualDeltaTooLarge,
                detail: "execution replanning requires positive poly fills".to_owned(),
            });
        }
        let poly_notional = UsdNotional(
            actual_fills
                .iter()
                .map(|fill| fill.notional_usd.0)
                .sum::<Decimal>(),
        );
        let requested_qty = CexBaseQty(
            parent_plan.cex_planned_qty.0 * actual_poly_shares / parent_plan.poly_planned_shares.0,
        );
        let normalized_qty = requested_qty.floor_to_step(context.cex_qty_step);
        if normalized_qty.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ResidualDeltaTooLarge,
                detail: "replanned hedge qty floors to zero".to_owned(),
            });
        }
        let cex_estimate = self.estimate_cex_order(&context, parent_plan.cex_side, normalized_qty);
        if cex_estimate.status == polyalpha_core::OrderStatus::Rejected {
            return Err(PlanRejection {
                reason: map_estimate_rejection(cex_estimate.rejection_reason.as_deref()),
                detail: cex_estimate
                    .rejection_reason
                    .unwrap_or_else(|| "cex estimate rejected".to_owned()),
            });
        }
        let fill_suffix = actual_fills
            .iter()
            .map(|fill| fill.fill_id.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let plan_hash = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{fill_suffix}", parent_plan.plan_hash).as_bytes(),
        )
        .to_string();
        let max_scale =
            scale_ratio(poly_notional, planned_poly_notional(parent_plan)).max(scale_ratio(
                cex_estimate.executable_notional_usd,
                planned_cex_notional(parent_plan),
            ));
        let economics = scaled_economics_for_replan(
            parent_plan,
            self,
            poly_notional,
            cex_estimate.executable_notional_usd,
        );
        let avg_poly_price = if actual_poly_shares > Decimal::ZERO {
            Price(poly_notional.0 / actual_poly_shares)
        } else {
            parent_plan.poly_executable_price
        };

        Ok(TradePlan {
            plan_id: format!("plan-{plan_hash}"),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: format!("{}:{fill_suffix}", parent_plan.idempotency_key),
            correlation_id: parent_plan.correlation_id.clone(),
            plan_hash,
            intent_type: "delta_rebalance".to_owned(),
            priority: "delta_rebalance".to_owned(),
            recovery_decision_reason: parent_plan.recovery_decision_reason.clone(),
            created_at_ms: actual_fills
                .iter()
                .map(|fill| fill.timestamp_ms)
                .max()
                .unwrap_or(parent_plan.created_at_ms),
            poly_requested_shares: PolyShares(actual_poly_shares),
            poly_planned_shares: PolyShares(actual_poly_shares),
            poly_max_shares: PolyShares(actual_poly_shares),
            poly_max_cost_usd: poly_notional,
            poly_executable_price: avg_poly_price,
            cex_exchange_timestamp_ms: context.cex_book.exchange_timestamp_ms,
            cex_received_at_ms: context.cex_book.received_at_ms,
            cex_sequence: context.cex_book.sequence,
            cex_book_avg_price: cex_estimate
                .book_average_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, parent_plan.cex_side)),
            cex_executable_price: cex_estimate
                .executable_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, parent_plan.cex_side)),
            cex_planned_qty: normalized_qty,
            raw_edge_usd: economics.raw_edge_usd,
            poly_friction_cost_usd: economics.poly_friction_cost_usd,
            cex_friction_cost_usd: economics.cex_friction_cost_usd,
            poly_fee_usd: economics.poly_fee_usd,
            cex_fee_usd: economics.cex_fee_usd,
            expected_funding_cost_usd: economics.funding_cost_usd,
            residual_risk_penalty_usd: economics.residual_risk_penalty_usd,
            planned_edge_usd: economics.realized_edge_usd,
            post_rounding_residual_delta: scale_f64(
                parent_plan.post_rounding_residual_delta,
                max_scale,
            ),
            shock_loss_up_1pct: economics.shock_loss_up_1pct,
            shock_loss_down_1pct: economics.shock_loss_down_1pct,
            shock_loss_up_2pct: economics.shock_loss_up_2pct,
            shock_loss_down_2pct: economics.shock_loss_down_2pct,
            max_plan_vs_fill_deviation_usd: scale_usd(
                parent_plan.max_plan_vs_fill_deviation_usd,
                max_scale,
            ),
            ..parent_plan.clone()
        })
    }

    fn validate_context(&self, context: &CanonicalPlanningContext) -> Result<(), PlanRejection> {
        Self::validate_book(&context.poly_yes_book, true)?;
        Self::validate_book(&context.poly_no_book, true)?;
        Self::validate_book(&context.cex_book, false)?;
        Ok(())
    }

    fn validate_book(book: &OrderBookSnapshot, is_poly: bool) -> Result<(), PlanRejection> {
        if book.bids.is_empty() && book.asks.is_empty() {
            return Err(PlanRejection {
                reason: if is_poly {
                    PlanRejectionReason::MissingPolyBook
                } else {
                    PlanRejectionReason::MissingCexBook
                },
                detail: "book is missing both sides".to_owned(),
            });
        }
        if book.bids.is_empty() || book.asks.is_empty() {
            return Err(PlanRejection {
                reason: if is_poly {
                    PlanRejectionReason::OneSidedPolyBook
                } else {
                    PlanRejectionReason::OneSidedCexBook
                },
                detail: "book is one sided".to_owned(),
            });
        }
        Ok(())
    }

    fn plan_open(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        let PlanningIntent::OpenPosition {
            candidate,
            max_budget_usd,
            max_residual_delta,
            max_shock_loss_usd,
            ..
        } = intent
        else {
            unreachable!("plan_open only accepts open intents");
        };

        let plan_hash = stable_plan_hash(intent, context);
        let poly_book = self.poly_book_for_candidate(context, candidate);
        let poly_best_ask = poly_book
            .asks
            .iter()
            .min_by(|left, right| left.price.cmp(&right.price))
            .map(|level| level.price)
            .ok_or_else(|| PlanRejection {
                reason: PlanRejectionReason::MissingPolyBook,
                detail: "poly book missing asks".to_owned(),
            })?;

        let budget_cap_usd = Decimal::from_f64((*max_budget_usd).min(candidate.risk_budget_usd))
            .unwrap_or(Decimal::ZERO);
        let delta_abs = Decimal::from_f64(candidate.delta_estimate.abs()).unwrap_or(Decimal::ZERO);
        if delta_abs <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ZeroCexHedgeQty,
                detail: "open candidate cannot produce a non-zero cex hedge".to_owned(),
            });
        }
        let max_shares_by_budget = if poly_best_ask.0 > Decimal::ZERO {
            budget_cap_usd / poly_best_ask.0
        } else {
            Decimal::ZERO
        };

        let cex_side = cex_side_for_delta(candidate.delta_estimate);
        let cex_depth = self.available_cex_qty(&context.cex_book, cex_side);
        let max_shares_by_cex = if delta_abs > Decimal::ZERO {
            cex_depth / delta_abs
        } else {
            max_shares_by_budget
        };
        let capped_max_shares = PolyShares(max_shares_by_budget.min(max_shares_by_cex));
        if capped_max_shares.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: if cex_depth <= Decimal::ZERO {
                    PlanRejectionReason::InsufficientCexDepth
                } else {
                    PlanRejectionReason::InsufficientPolyDepth
                },
                detail: "no executable size remains after depth/budget constraints".to_owned(),
            });
        }

        let poly_estimate =
            self.estimate_poly_open(candidate, context, budget_cap_usd, capped_max_shares);
        if poly_estimate.status == polyalpha_core::OrderStatus::Rejected {
            return Err(PlanRejection {
                reason: map_estimate_rejection(poly_estimate.rejection_reason.as_deref()),
                detail: poly_estimate
                    .rejection_reason
                    .unwrap_or_else(|| "poly estimate rejected".to_owned()),
            });
        }

        let poly_planned_shares = match poly_estimate.filled_quantity {
            polyalpha_core::VenueQuantity::PolyShares(shares) => shares,
            polyalpha_core::VenueQuantity::CexBaseQty(_) => PolyShares::ZERO,
        };
        let exact_cex_qty = poly_planned_shares.to_cex_base_qty(candidate.delta_estimate);
        if exact_cex_qty.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ZeroCexHedgeQty,
                detail: "open plan cannot produce a non-zero cex hedge".to_owned(),
            });
        }
        let cex_planned_qty = exact_cex_qty.floor_to_step(context.cex_qty_step);
        let post_rounding_residual_qty = (exact_cex_qty.0 - cex_planned_qty.0).max(Decimal::ZERO);
        if cex_planned_qty.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ZeroCexHedgeQty,
                detail: "open plan cannot produce a non-zero cex hedge after step rounding"
                    .to_owned(),
            });
        }
        if cex_planned_qty.0 > cex_depth {
            return Err(PlanRejection {
                reason: PlanRejectionReason::InsufficientCexDepth,
                detail: "cex depth cannot hedge planned shares".to_owned(),
            });
        }

        let cex_estimate = if cex_planned_qty.0 > Decimal::ZERO {
            self.estimate_cex_order(context, cex_side, cex_planned_qty)
        } else {
            empty_estimate(
                context,
                context.cex_book.exchange,
                context.cex_book.symbol.clone(),
                context.cex_book.instrument,
                cex_side,
            )
        };
        let allowed_instant_loss_usd =
            budget_cap_usd * self.max_open_instant_loss_pct_of_budget.max(Decimal::ZERO);
        let selected_open = if self
            .instant_open_loss_usd(&poly_estimate, &cex_estimate)
            .0
            <= allowed_instant_loss_usd
        {
            OpenSizingSelection {
                budget_cap_usd,
                capped_max_shares,
                poly_estimate,
                poly_planned_shares,
                cex_planned_qty,
                post_rounding_residual_qty,
                cex_estimate,
            }
        } else {
            self.find_loss_bounded_open_selection(
                candidate,
                context,
                poly_best_ask,
                max_shares_by_cex,
                cex_side,
                budget_cap_usd,
                allowed_instant_loss_usd,
            )
            .ok_or_else(|| PlanRejection {
                reason: PlanRejectionReason::OpenInstantLossTooLarge,
                detail: format!(
                    "instant open loss exceeds configured cap of {allowed_instant_loss_usd} usd"
                ),
            })?
        };
        let OpenSizingSelection {
            budget_cap_usd,
            capped_max_shares,
            poly_estimate,
            poly_planned_shares,
            cex_planned_qty,
            post_rounding_residual_qty,
            cex_estimate,
        } = selected_open;
        let raw_edge_usd = poly_estimate.executable_notional_usd.0
            * Decimal::from_f64(candidate.raw_mispricing.abs()).unwrap_or(Decimal::ZERO);
        let hedge_notional_proxy = cex_estimate
            .executable_notional_usd
            .0
            .max(poly_estimate.executable_notional_usd.0 * delta_abs);
        let poly_fee_usd = bps_fee(poly_estimate.executable_notional_usd, self.poly_fee_bps);
        let cex_fee_usd = bps_fee(UsdNotional(hedge_notional_proxy), self.cex_fee_bps(false));
        let expected_funding_cost_usd = bps_fee(
            UsdNotional(hedge_notional_proxy),
            self.funding_bps_per_day(),
        );
        let residual_risk_penalty_usd = UsdNotional::ZERO;
        let planned_edge_usd = UsdNotional(
            UsdNotional(raw_edge_usd).0
                - poly_estimate
                    .friction_cost_usd
                    .unwrap_or(UsdNotional::ZERO)
                    .0
                - cex_estimate
                    .friction_cost_usd
                    .unwrap_or(UsdNotional::ZERO)
                    .0
                - poly_fee_usd.0
                - cex_fee_usd.0
                - expected_funding_cost_usd.0
                - residual_risk_penalty_usd.0,
        );
        if planned_edge_usd.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::NonPositivePlannedEdge,
                detail: "planned edge is not positive after fees and friction".to_owned(),
            });
        }

        let post_rounding_residual_delta = post_rounding_residual_qty.to_f64().unwrap_or_default();
        let max_residual_delta = (*max_residual_delta).max(0.0);
        if post_rounding_residual_delta > max_residual_delta {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ResidualDeltaTooLarge,
                detail: "post-rounding residual delta exceeds intent threshold".to_owned(),
            });
        }

        let shock_reference_price = cex_estimate
            .executable_price
            .unwrap_or_else(|| best_level_price(&context.cex_book, cex_side));
        let shock_base = UsdNotional(post_rounding_residual_qty * shock_reference_price.0);
        let shock_loss_up_1pct = bps_fee(shock_base, 100);
        let shock_loss_down_1pct = shock_loss_up_1pct;
        let shock_loss_up_2pct = bps_fee(shock_base, 200);
        let shock_loss_down_2pct = shock_loss_up_2pct;
        let max_shock_loss = Decimal::from_f64(*max_shock_loss_usd).unwrap_or(Decimal::ZERO);
        if shock_loss_up_2pct.0 > max_shock_loss || shock_loss_down_2pct.0 > max_shock_loss {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ShockLossTooLarge,
                detail: "shock loss exceeds intent threshold".to_owned(),
            });
        }

        Ok(TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: format!("plan-{plan_hash}"),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: plan_hash.clone(),
            correlation_id: intent.correlation_id().to_owned(),
            symbol: intent.symbol().clone(),
            intent_type: intent.intent_type_code().to_owned(),
            priority: "open_position".to_owned(),
            recovery_decision_reason: recovery_reason_from_intent(intent),
            created_at_ms: context.now_ms,
            poly_exchange_timestamp_ms: poly_book.exchange_timestamp_ms,
            poly_received_at_ms: poly_book.received_at_ms,
            poly_sequence: poly_book.sequence,
            cex_exchange_timestamp_ms: context.cex_book.exchange_timestamp_ms,
            cex_received_at_ms: context.cex_book.received_at_ms,
            cex_sequence: context.cex_book.sequence,
            plan_hash,
            poly_side: OrderSide::Buy,
            poly_token_side: candidate.token_side,
            poly_sizing_mode: "buy_budget_cap".to_owned(),
            poly_requested_shares: capped_max_shares,
            poly_planned_shares,
            poly_max_cost_usd: UsdNotional(budget_cap_usd),
            poly_max_avg_price: poly_estimate.executable_price.unwrap_or(poly_best_ask),
            poly_max_shares: capped_max_shares,
            poly_min_avg_price: Price::ZERO,
            poly_min_proceeds_usd: UsdNotional::ZERO,
            poly_book_avg_price: poly_estimate.book_average_price.unwrap_or(poly_best_ask),
            poly_executable_price: poly_estimate.executable_price.unwrap_or(poly_best_ask),
            poly_friction_cost_usd: poly_estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO),
            poly_fee_usd,
            cex_side,
            cex_planned_qty,
            cex_book_avg_price: cex_estimate
                .book_average_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, cex_side)),
            cex_executable_price: cex_estimate
                .executable_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, cex_side)),
            cex_friction_cost_usd: cex_estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO),
            cex_fee_usd,
            raw_edge_usd: UsdNotional(raw_edge_usd),
            planned_edge_usd,
            expected_funding_cost_usd,
            residual_risk_penalty_usd,
            post_rounding_residual_delta,
            shock_loss_up_1pct,
            shock_loss_down_1pct,
            shock_loss_up_2pct,
            shock_loss_down_2pct,
            plan_ttl_ms: DEFAULT_PLAN_TTL_MS,
            max_poly_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_cex_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_poly_price_move: default_max_poly_price_move(),
            max_cex_price_move: default_max_cex_price_move(),
            min_planned_edge_usd: UsdNotional(Decimal::new(1, 2)),
            max_residual_delta,
            max_shock_loss_usd: UsdNotional(max_shock_loss),
            max_plan_vs_fill_deviation_usd: UsdNotional(Decimal::new(5, 0)),
        })
    }

    fn plan_flatten(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
        priority: &str,
        poly_sizing_mode: &str,
    ) -> Result<TradePlan, PlanRejection> {
        let close_ratio = close_ratio_for_intent(intent);
        let (poly_token_side, requested_poly_shares) = flatten_poly_position(context, close_ratio)?;
        let desired_cex_qty = flatten_cex_position(context, close_ratio);
        let poly_book = match poly_token_side {
            TokenSide::No => &context.poly_no_book,
            TokenSide::Yes => &context.poly_yes_book,
        };
        let poly_estimate = if requested_poly_shares.0 > Decimal::ZERO {
            let estimate = self.estimate_poly_close(
                intent.symbol(),
                poly_token_side,
                context,
                requested_poly_shares,
            );
            if estimate.status == polyalpha_core::OrderStatus::Rejected {
                return Err(PlanRejection {
                    reason: map_estimate_rejection(estimate.rejection_reason.as_deref()),
                    detail: estimate
                        .rejection_reason
                        .unwrap_or_else(|| "poly close estimate rejected".to_owned()),
                });
            }
            estimate
        } else {
            empty_estimate(
                context,
                poly_book.exchange,
                poly_book.symbol.clone(),
                poly_book.instrument,
                OrderSide::Sell,
            )
        };
        let cex_side = flatten_cex_side(context.current_cex_net_qty);
        let cex_estimate = if desired_cex_qty.0 > Decimal::ZERO {
            self.estimate_cex_order(context, cex_side, desired_cex_qty)
        } else {
            empty_estimate(
                context,
                context.cex_book.exchange,
                context.cex_book.symbol.clone(),
                context.cex_book.instrument,
                cex_side,
            )
        };
        let poly_fee_usd = bps_fee(poly_estimate.executable_notional_usd, self.poly_fee_bps);
        let cex_fee_usd = bps_fee(
            cex_estimate.executable_notional_usd,
            self.cex_fee_bps(false),
        );
        let planned_edge_usd = UsdNotional(
            -poly_estimate
                .friction_cost_usd
                .unwrap_or(UsdNotional::ZERO)
                .0
                - cex_estimate
                    .friction_cost_usd
                    .unwrap_or(UsdNotional::ZERO)
                    .0
                - poly_fee_usd.0
                - cex_fee_usd.0,
        );
        let plan_hash = stable_plan_hash(intent, context);
        Ok(TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: format!("plan-{plan_hash}"),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: plan_hash.clone(),
            correlation_id: intent.correlation_id().to_owned(),
            symbol: intent.symbol().clone(),
            intent_type: intent.intent_type_code().to_owned(),
            priority: priority.to_owned(),
            recovery_decision_reason: recovery_reason_from_intent(intent),
            created_at_ms: context.now_ms,
            poly_exchange_timestamp_ms: context.poly_yes_book.exchange_timestamp_ms,
            poly_received_at_ms: context.poly_yes_book.received_at_ms,
            poly_sequence: context.poly_yes_book.sequence,
            cex_exchange_timestamp_ms: context.cex_book.exchange_timestamp_ms,
            cex_received_at_ms: context.cex_book.received_at_ms,
            cex_sequence: context.cex_book.sequence,
            plan_hash,
            poly_side: OrderSide::Sell,
            poly_token_side,
            poly_sizing_mode: poly_sizing_mode.to_owned(),
            poly_requested_shares: requested_poly_shares,
            poly_planned_shares: match poly_estimate.filled_quantity {
                polyalpha_core::VenueQuantity::PolyShares(shares) => shares,
                polyalpha_core::VenueQuantity::CexBaseQty(_) => PolyShares::ZERO,
            },
            poly_max_cost_usd: UsdNotional::ZERO,
            poly_max_avg_price: Price::ZERO,
            poly_max_shares: PolyShares::ZERO,
            poly_min_avg_price: poly_estimate.executable_price.unwrap_or(Price::ZERO),
            poly_min_proceeds_usd: UsdNotional::ZERO,
            poly_book_avg_price: poly_estimate
                .book_average_price
                .unwrap_or_else(|| best_level_price(poly_book, OrderSide::Sell)),
            poly_executable_price: poly_estimate
                .executable_price
                .unwrap_or_else(|| best_level_price(poly_book, OrderSide::Sell)),
            poly_friction_cost_usd: poly_estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO),
            poly_fee_usd,
            cex_side,
            cex_planned_qty: desired_cex_qty,
            cex_book_avg_price: cex_estimate
                .book_average_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, cex_side)),
            cex_executable_price: cex_estimate
                .executable_price
                .unwrap_or_else(|| best_level_price(&context.cex_book, cex_side)),
            cex_friction_cost_usd: cex_estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO),
            cex_fee_usd,
            raw_edge_usd: UsdNotional::ZERO,
            planned_edge_usd,
            expected_funding_cost_usd: UsdNotional::ZERO,
            residual_risk_penalty_usd: UsdNotional::ZERO,
            post_rounding_residual_delta: 0.0,
            shock_loss_up_1pct: UsdNotional::ZERO,
            shock_loss_down_1pct: UsdNotional::ZERO,
            shock_loss_up_2pct: UsdNotional::ZERO,
            shock_loss_down_2pct: UsdNotional::ZERO,
            plan_ttl_ms: DEFAULT_PLAN_TTL_MS,
            max_poly_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_cex_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_poly_price_move: default_max_poly_price_move(),
            max_cex_price_move: default_max_cex_price_move(),
            min_planned_edge_usd: UsdNotional::ZERO,
            max_residual_delta: 0.0,
            max_shock_loss_usd: UsdNotional::ZERO,
            max_plan_vs_fill_deviation_usd: UsdNotional::ZERO,
        })
    }

    fn plan_residual_recovery(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        let PlanningIntent::ResidualRecovery {
            residual_snapshot,
            recovery_reason,
            ..
        } = intent
        else {
            unreachable!("plan_residual_recovery only accepts residual recovery intents");
        };

        match recovery_reason {
            RecoveryDecisionReason::CexTopUpCheaper | RecoveryDecisionReason::CexTopUpFaster => {
                self.plan_cex_top_up(intent, context, residual_snapshot)
            }
            _ => self.plan_flatten(intent, context, "residual_recovery", "sell_exact_shares"),
        }
    }

    fn plan_delta_rebalance(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        let PlanningIntent::DeltaRebalance {
            residual_snapshot, ..
        } = intent
        else {
            unreachable!("plan_delta_rebalance only accepts delta rebalance intents");
        };

        match self.plan_cex_top_up(intent, context, residual_snapshot) {
            Ok(mut plan) => {
                plan.priority = "delta_rebalance".to_owned();
                Ok(plan)
            }
            Err(rejection)
                if matches!(
                    rejection.reason,
                    PlanRejectionReason::ResidualDeltaTooLarge
                        | PlanRejectionReason::MissingCexBook
                        | PlanRejectionReason::InsufficientCexDepth
                        | PlanRejectionReason::AdapterCannotPreserveConstraints
                ) =>
            {
                Err(rejection)
            }
            Err(_) => self.plan_flatten(intent, context, "delta_rebalance", "sell_exact_shares"),
        }
    }

    fn plan_cex_top_up(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
        snapshot: &ResidualSnapshot,
    ) -> Result<TradePlan, PlanRejection> {
        let requested_qty = CexBaseQty(
            Decimal::from_f64(snapshot.residual_delta)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO),
        );
        let cex_planned_qty = requested_qty.floor_to_step(context.cex_qty_step);
        if cex_planned_qty.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ResidualDeltaTooLarge,
                detail: "residual recovery qty floors to zero".to_owned(),
            });
        }
        let cex_estimate =
            self.estimate_cex_order(context, snapshot.preferred_cex_side, cex_planned_qty);
        if cex_estimate.status == polyalpha_core::OrderStatus::Rejected {
            return Err(PlanRejection {
                reason: map_estimate_rejection(cex_estimate.rejection_reason.as_deref()),
                detail: cex_estimate
                    .rejection_reason
                    .unwrap_or_else(|| "cex top-up estimate rejected".to_owned()),
            });
        }
        let plan_hash = stable_plan_hash(intent, context);
        let residual_after_rounding = (requested_qty.0 - cex_planned_qty.0)
            .max(Decimal::ZERO)
            .to_f64()
            .unwrap_or_default();
        let cex_fee_usd = bps_fee(
            cex_estimate.executable_notional_usd,
            self.cex_fee_bps(false),
        );
        let funding_cost = bps_fee(
            cex_estimate.executable_notional_usd,
            self.funding_bps_per_day(),
        );
        let planned_edge_usd = UsdNotional(
            -cex_estimate
                .friction_cost_usd
                .unwrap_or(UsdNotional::ZERO)
                .0
                - cex_fee_usd.0
                - funding_cost.0,
        );
        let poly_token_side = if snapshot.current_poly_yes_shares.0 > Decimal::ZERO {
            TokenSide::Yes
        } else {
            TokenSide::No
        };

        Ok(TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: format!("plan-{plan_hash}"),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: plan_hash.clone(),
            correlation_id: intent.correlation_id().to_owned(),
            symbol: intent.symbol().clone(),
            intent_type: intent.intent_type_code().to_owned(),
            priority: "residual_recovery".to_owned(),
            recovery_decision_reason: recovery_reason_from_intent(intent),
            created_at_ms: context.now_ms,
            poly_exchange_timestamp_ms: selected_poly_book(context, poly_token_side)
                .exchange_timestamp_ms,
            poly_received_at_ms: selected_poly_book(context, poly_token_side).received_at_ms,
            poly_sequence: selected_poly_book(context, poly_token_side).sequence,
            cex_exchange_timestamp_ms: context.cex_book.exchange_timestamp_ms,
            cex_received_at_ms: context.cex_book.received_at_ms,
            cex_sequence: context.cex_book.sequence,
            plan_hash,
            poly_side: OrderSide::Sell,
            poly_token_side,
            poly_sizing_mode: "sell_exact_shares".to_owned(),
            poly_requested_shares: PolyShares::ZERO,
            poly_planned_shares: PolyShares::ZERO,
            poly_max_cost_usd: UsdNotional::ZERO,
            poly_max_avg_price: Price::ZERO,
            poly_max_shares: PolyShares::ZERO,
            poly_min_avg_price: Price::ZERO,
            poly_min_proceeds_usd: UsdNotional::ZERO,
            poly_book_avg_price: Price::ZERO,
            poly_executable_price: Price::ZERO,
            poly_friction_cost_usd: UsdNotional::ZERO,
            poly_fee_usd: UsdNotional::ZERO,
            cex_side: snapshot.preferred_cex_side,
            cex_planned_qty,
            cex_book_avg_price: cex_estimate.book_average_price.unwrap_or_else(|| {
                best_level_price(&context.cex_book, snapshot.preferred_cex_side)
            }),
            cex_executable_price: cex_estimate.executable_price.unwrap_or_else(|| {
                best_level_price(&context.cex_book, snapshot.preferred_cex_side)
            }),
            cex_friction_cost_usd: cex_estimate.friction_cost_usd.unwrap_or(UsdNotional::ZERO),
            cex_fee_usd,
            raw_edge_usd: UsdNotional::ZERO,
            planned_edge_usd,
            expected_funding_cost_usd: funding_cost,
            residual_risk_penalty_usd: UsdNotional::ZERO,
            post_rounding_residual_delta: residual_after_rounding,
            shock_loss_up_1pct: bps_fee(
                UsdNotional(
                    cex_planned_qty.0
                        * best_level_price(&context.cex_book, snapshot.preferred_cex_side).0,
                ),
                100,
            ),
            shock_loss_down_1pct: bps_fee(
                UsdNotional(
                    cex_planned_qty.0
                        * best_level_price(&context.cex_book, snapshot.preferred_cex_side).0,
                ),
                100,
            ),
            shock_loss_up_2pct: bps_fee(
                UsdNotional(
                    cex_planned_qty.0
                        * best_level_price(&context.cex_book, snapshot.preferred_cex_side).0,
                ),
                200,
            ),
            shock_loss_down_2pct: bps_fee(
                UsdNotional(
                    cex_planned_qty.0
                        * best_level_price(&context.cex_book, snapshot.preferred_cex_side).0,
                ),
                200,
            ),
            plan_ttl_ms: DEFAULT_PLAN_TTL_MS,
            max_poly_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_cex_sequence_drift: DEFAULT_MAX_SEQUENCE_DRIFT,
            max_poly_price_move: default_max_poly_price_move(),
            max_cex_price_move: default_max_cex_price_move(),
            min_planned_edge_usd: UsdNotional::ZERO,
            max_residual_delta: 0.0,
            max_shock_loss_usd: UsdNotional::ZERO,
            max_plan_vs_fill_deviation_usd: UsdNotional::ZERO,
        })
    }

    fn poly_book_for_candidate<'a>(
        &self,
        context: &'a CanonicalPlanningContext,
        candidate: &OpenCandidate,
    ) -> &'a OrderBookSnapshot {
        match candidate.token_side {
            polyalpha_core::TokenSide::Yes => &context.poly_yes_book,
            polyalpha_core::TokenSide::No => &context.poly_no_book,
        }
    }

    fn available_cex_qty(&self, book: &OrderBookSnapshot, side: OrderSide) -> Decimal {
        let levels = match side {
            OrderSide::Buy => &book.asks,
            OrderSide::Sell => &book.bids,
        };
        levels
            .iter()
            .map(|level| match level.quantity {
                polyalpha_core::VenueQuantity::CexBaseQty(qty) => qty.0,
                polyalpha_core::VenueQuantity::PolyShares(_) => Decimal::ZERO,
            })
            .sum()
    }

    fn estimate_poly_open(
        &self,
        candidate: &OpenCandidate,
        context: &CanonicalPlanningContext,
        budget_cap_usd: Decimal,
        capped_max_shares: PolyShares,
    ) -> OrderExecutionEstimate {
        let executor = self.dry_run_executor(context);

        executor.estimate_order_request(&OrderRequest::Poly(PolyOrderRequest {
            client_order_id: polyalpha_core::ClientOrderId("planner-poly-open".to_owned()),
            symbol: candidate.symbol.clone(),
            token_side: candidate.token_side,
            side: OrderSide::Buy,
            order_type: polyalpha_core::OrderType::Market,
            sizing: PolySizingInstruction::BuyBudgetCap {
                max_cost_usd: UsdNotional(budget_cap_usd),
                max_avg_price: Price::ONE,
                max_shares: capped_max_shares,
            },
            time_in_force: polyalpha_core::TimeInForce::Fok,
            post_only: false,
        }))
    }

    fn estimate_poly_close(
        &self,
        symbol: &Symbol,
        token_side: TokenSide,
        context: &CanonicalPlanningContext,
        shares: PolyShares,
    ) -> OrderExecutionEstimate {
        let executor = self.dry_run_executor(context);

        executor.estimate_order_request(&OrderRequest::Poly(PolyOrderRequest {
            client_order_id: polyalpha_core::ClientOrderId("planner-poly-close".to_owned()),
            symbol: symbol.clone(),
            token_side,
            side: OrderSide::Sell,
            order_type: polyalpha_core::OrderType::Market,
            sizing: PolySizingInstruction::SellExactShares {
                shares,
                min_avg_price: Price::ZERO,
            },
            time_in_force: polyalpha_core::TimeInForce::Fok,
            post_only: false,
        }))
    }

    fn dry_run_executor(&self, context: &CanonicalPlanningContext) -> DryRunExecutor {
        let provider = Arc::new(InMemoryOrderbookProvider::new());
        provider.update(context.poly_yes_book.clone());
        provider.update(context.poly_no_book.clone());
        provider.update_cex(context.cex_book.clone(), context.cex_book.symbol.0.clone());

        DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                poly_slippage_bps: self.poly_slippage_bps as u64,
                cex_slippage_bps: self.cex_slippage_bps as u64,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        )
    }

    fn estimate_cex_order(
        &self,
        context: &CanonicalPlanningContext,
        side: OrderSide,
        qty: CexBaseQty,
    ) -> OrderExecutionEstimate {
        let levels = match side {
            OrderSide::Buy => &context.cex_book.asks,
            OrderSide::Sell => &context.cex_book.bids,
        };
        let mut remaining = qty.0;
        let mut total_cost = Decimal::ZERO;
        let mut filled = Decimal::ZERO;
        for level in levels {
            let available = match level.quantity {
                polyalpha_core::VenueQuantity::CexBaseQty(base_qty) => base_qty.0,
                polyalpha_core::VenueQuantity::PolyShares(_) => Decimal::ZERO,
            };
            let take = remaining.min(available);
            if take <= Decimal::ZERO {
                continue;
            }
            total_cost += take * level.price.0;
            filled += take;
            remaining -= take;
            if remaining <= Decimal::ZERO {
                break;
            }
        }
        let book_average_price = if filled > Decimal::ZERO {
            Some(Price(total_cost / filled))
        } else {
            None
        };
        let executable_price = book_average_price
            .map(|price| apply_linear_slippage(price, side, self.cex_slippage_bps.into(), false));
        let executable_notional_usd = match executable_price {
            Some(price) => UsdNotional(filled * price.0),
            None => UsdNotional::ZERO,
        };

        OrderExecutionEstimate {
            exchange: context.cex_book.exchange,
            symbol: context.cex_book.symbol.clone(),
            instrument: context.cex_book.instrument,
            side,
            status: if remaining <= Decimal::ZERO {
                polyalpha_core::OrderStatus::Filled
            } else {
                polyalpha_core::OrderStatus::Rejected
            },
            requested_quantity: polyalpha_core::VenueQuantity::CexBaseQty(qty),
            filled_quantity: polyalpha_core::VenueQuantity::CexBaseQty(CexBaseQty(filled)),
            orderbook_mid_price: mid_price(&context.cex_book),
            book_average_price,
            executable_price,
            executable_notional_usd,
            friction_cost_usd: Some(bps_fee(executable_notional_usd, self.cex_slippage_bps)),
            fee_usd: Some(bps_fee(executable_notional_usd, self.cex_fee_bps(false))),
            rejection_reason: if remaining <= Decimal::ZERO {
                None
            } else {
                Some("insufficient_cex_depth".to_owned())
            },
            is_complete: remaining <= Decimal::ZERO,
        }
    }

    fn instant_open_loss_usd(
        &self,
        poly_estimate: &OrderExecutionEstimate,
        cex_estimate: &OrderExecutionEstimate,
    ) -> UsdNotional {
        UsdNotional(
            poly_estimate
                .friction_cost_usd
                .unwrap_or(UsdNotional::ZERO)
                .0
                + cex_estimate
                    .friction_cost_usd
                    .unwrap_or(UsdNotional::ZERO)
                    .0
                + bps_fee(poly_estimate.executable_notional_usd, self.poly_fee_bps).0
                + bps_fee(cex_estimate.executable_notional_usd, self.cex_fee_bps(false)).0,
        )
    }

    fn find_loss_bounded_open_selection(
        &self,
        candidate: &OpenCandidate,
        context: &CanonicalPlanningContext,
        poly_best_ask: Price,
        max_shares_by_cex: Decimal,
        cex_side: OrderSide,
        budget_cap_usd: Decimal,
        allowed_instant_loss_usd: Decimal,
    ) -> Option<OpenSizingSelection> {
        let mut low = Decimal::ZERO;
        let mut high = budget_cap_usd;
        let mut best = None;

        for _ in 0..OPEN_INSTANT_LOSS_BINARY_SEARCH_STEPS {
            let candidate_budget = (low + high) / Decimal::from(2u32);
            if candidate_budget <= Decimal::ZERO {
                break;
            }

            match self.open_selection_for_budget(
                candidate,
                context,
                poly_best_ask,
                max_shares_by_cex,
                cex_side,
                candidate_budget,
            ) {
                OpenSizingProbe::BelowExecutableFloor => {
                    low = candidate_budget;
                    continue;
                }
                OpenSizingProbe::Rejected => {
                    high = candidate_budget;
                    continue;
                }
                OpenSizingProbe::Candidate(selection) => {
                    if self
                        .instant_open_loss_usd(&selection.poly_estimate, &selection.cex_estimate)
                        .0
                        <= allowed_instant_loss_usd
                    {
                        low = candidate_budget;
                        best = Some(selection);
                    } else {
                        high = candidate_budget;
                    }
                }
            }
        }

        best
    }

    fn open_selection_for_budget(
        &self,
        candidate: &OpenCandidate,
        context: &CanonicalPlanningContext,
        poly_best_ask: Price,
        max_shares_by_cex: Decimal,
        cex_side: OrderSide,
        budget_cap_usd: Decimal,
    ) -> OpenSizingProbe {
        let max_shares_by_budget = if poly_best_ask.0 > Decimal::ZERO {
            budget_cap_usd / poly_best_ask.0
        } else {
            Decimal::ZERO
        };
        let capped_max_shares = PolyShares(max_shares_by_budget.min(max_shares_by_cex));
        if capped_max_shares.0 <= Decimal::ZERO {
            return OpenSizingProbe::BelowExecutableFloor;
        }

        let poly_estimate =
            self.estimate_poly_open(candidate, context, budget_cap_usd, capped_max_shares);
        if poly_estimate.status == polyalpha_core::OrderStatus::Rejected {
            return OpenSizingProbe::Rejected;
        }

        let poly_planned_shares = match poly_estimate.filled_quantity {
            polyalpha_core::VenueQuantity::PolyShares(shares) => shares,
            polyalpha_core::VenueQuantity::CexBaseQty(_) => PolyShares::ZERO,
        };
        if poly_planned_shares.0 <= Decimal::ZERO {
            return OpenSizingProbe::BelowExecutableFloor;
        }

        let exact_cex_qty = poly_planned_shares.to_cex_base_qty(candidate.delta_estimate);
        if exact_cex_qty.0 <= Decimal::ZERO {
            return OpenSizingProbe::BelowExecutableFloor;
        }

        let cex_planned_qty = exact_cex_qty.floor_to_step(context.cex_qty_step);
        if cex_planned_qty.0 <= Decimal::ZERO {
            return OpenSizingProbe::BelowExecutableFloor;
        }

        let cex_estimate = self.estimate_cex_order(context, cex_side, cex_planned_qty);
        if cex_estimate.status == polyalpha_core::OrderStatus::Rejected {
            return OpenSizingProbe::Rejected;
        }

        OpenSizingProbe::Candidate(OpenSizingSelection {
            budget_cap_usd,
            capped_max_shares,
            poly_estimate,
            poly_planned_shares,
            cex_planned_qty,
            post_rounding_residual_qty: (exact_cex_qty.0 - cex_planned_qty.0).max(Decimal::ZERO),
            cex_estimate,
        })
    }

    fn current_revalidation_metrics(
        &self,
        plan: &TradePlan,
        context: &CanonicalPlanningContext,
    ) -> Option<(UsdNotional, [UsdNotional; 4])> {
        let poly_estimate = self.estimate_poly_from_plan(plan, context)?;
        let cex_estimate = if plan.cex_planned_qty.0 > Decimal::ZERO {
            let estimate = self.estimate_cex_order(context, plan.cex_side, plan.cex_planned_qty);
            if estimate.status == polyalpha_core::OrderStatus::Rejected {
                return None;
            }
            estimate
        } else {
            empty_estimate(
                context,
                context.cex_book.exchange,
                context.cex_book.symbol.clone(),
                context.cex_book.instrument,
                plan.cex_side,
            )
        };
        let economics = scaled_economics_for_replan(
            plan,
            self,
            poly_estimate.executable_notional_usd,
            cex_estimate.executable_notional_usd,
        );
        let shock_reference_price = cex_estimate
            .executable_price
            .unwrap_or_else(|| best_level_price(&context.cex_book, plan.cex_side));
        let shock_losses = shock_losses_for_residual_delta(
            plan.post_rounding_residual_delta,
            shock_reference_price,
        );
        Some((economics.realized_edge_usd, shock_losses))
    }

    fn estimate_poly_from_plan(
        &self,
        plan: &TradePlan,
        context: &CanonicalPlanningContext,
    ) -> Option<OrderExecutionEstimate> {
        if plan.poly_side == OrderSide::Buy && plan.poly_max_shares.0 <= Decimal::ZERO {
            return Some(empty_estimate(
                context,
                selected_poly_book(context, plan.poly_token_side).exchange,
                plan.symbol.clone(),
                selected_poly_book(context, plan.poly_token_side).instrument,
                plan.poly_side,
            ));
        }
        if plan.poly_side == OrderSide::Sell && plan.poly_planned_shares.0 <= Decimal::ZERO {
            return Some(empty_estimate(
                context,
                selected_poly_book(context, plan.poly_token_side).exchange,
                plan.symbol.clone(),
                selected_poly_book(context, plan.poly_token_side).instrument,
                plan.poly_side,
            ));
        }

        let executor = self.dry_run_executor(context);
        let sizing = match plan.poly_sizing_mode.as_str() {
            "sell_min_proceeds" => PolySizingInstruction::SellMinProceeds {
                shares: plan.poly_planned_shares,
                min_proceeds_usd: plan.poly_min_proceeds_usd,
            },
            "sell_exact_shares" => PolySizingInstruction::SellExactShares {
                shares: plan.poly_planned_shares,
                min_avg_price: plan.poly_min_avg_price,
            },
            _ => PolySizingInstruction::BuyBudgetCap {
                max_cost_usd: plan.poly_max_cost_usd,
                max_avg_price: plan.poly_max_avg_price,
                max_shares: plan.poly_max_shares,
            },
        };
        let estimate = executor.estimate_order_request(&OrderRequest::Poly(PolyOrderRequest {
            client_order_id: polyalpha_core::ClientOrderId("planner-poly-revalidate".to_owned()),
            symbol: plan.symbol.clone(),
            token_side: plan.poly_token_side,
            side: plan.poly_side,
            order_type: polyalpha_core::OrderType::Market,
            sizing,
            time_in_force: polyalpha_core::TimeInForce::Fok,
            post_only: false,
        }));
        if estimate.status == polyalpha_core::OrderStatus::Rejected {
            None
        } else {
            Some(estimate)
        }
    }
}

fn stable_plan_hash(intent: &PlanningIntent, context: &CanonicalPlanningContext) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &context.canonical_bytes(intent)).to_string()
}

fn default_max_poly_price_move() -> Price {
    Price(Decimal::new(2, 2))
}

fn default_max_cex_price_move() -> Price {
    Price(Decimal::new(50, 0))
}

fn cex_side_for_delta(delta_estimate: f64) -> OrderSide {
    if delta_estimate >= 0.0 {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn close_ratio_for_intent(intent: &PlanningIntent) -> Decimal {
    let ratio = match intent {
        PlanningIntent::ClosePosition {
            target_close_ratio, ..
        } => (*target_close_ratio).clamp(0.0, 1.0),
        _ => 1.0,
    };
    Decimal::from_f64(ratio).unwrap_or(Decimal::ONE)
}

fn flatten_poly_position(
    context: &CanonicalPlanningContext,
    close_ratio: Decimal,
) -> Result<(TokenSide, PolyShares), PlanRejection> {
    let yes = context.current_poly_yes_shares.max(Decimal::ZERO);
    let no = context.current_poly_no_shares.max(Decimal::ZERO);
    match (yes > Decimal::ZERO, no > Decimal::ZERO) {
        (true, false) => Ok((TokenSide::Yes, PolyShares(yes * close_ratio))),
        (false, true) => Ok((TokenSide::No, PolyShares(no * close_ratio))),
        (false, false) => Ok((TokenSide::Yes, PolyShares::ZERO)),
        (true, true) => Err(PlanRejection {
            reason: PlanRejectionReason::AdapterCannotPreserveConstraints,
            detail: "multi-leg poly close is not supported in a single trade plan".to_owned(),
        }),
    }
}

fn flatten_cex_position(context: &CanonicalPlanningContext, close_ratio: Decimal) -> CexBaseQty {
    CexBaseQty(context.current_cex_net_qty.abs() * close_ratio)
}

fn flatten_cex_side(current_cex_net_qty: Decimal) -> OrderSide {
    if current_cex_net_qty >= Decimal::ZERO {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn map_estimate_rejection(reason: Option<&str>) -> PlanRejectionReason {
    match reason {
        Some("insufficient_poly_depth") => PlanRejectionReason::InsufficientPolyDepth,
        Some("poly_max_price_exceeded") => PlanRejectionReason::PolyMaxPriceExceeded,
        Some("poly_min_proceeds_not_met") => PlanRejectionReason::PolyMinProceedsNotMet,
        Some("insufficient_cex_depth") => PlanRejectionReason::InsufficientCexDepth,
        _ => PlanRejectionReason::NonPositivePlannedEdge,
    }
}

fn empty_estimate(
    context: &CanonicalPlanningContext,
    exchange: polyalpha_core::Exchange,
    symbol: Symbol,
    instrument: polyalpha_core::InstrumentKind,
    side: OrderSide,
) -> OrderExecutionEstimate {
    OrderExecutionEstimate {
        exchange,
        symbol,
        instrument,
        side,
        status: polyalpha_core::OrderStatus::Filled,
        requested_quantity: polyalpha_core::VenueQuantity::PolyShares(PolyShares::ZERO),
        filled_quantity: polyalpha_core::VenueQuantity::PolyShares(PolyShares::ZERO),
        orderbook_mid_price: mid_price(match instrument {
            polyalpha_core::InstrumentKind::PolyYes => &context.poly_yes_book,
            polyalpha_core::InstrumentKind::PolyNo => &context.poly_no_book,
            polyalpha_core::InstrumentKind::CexPerp => &context.cex_book,
        }),
        book_average_price: Some(Price::ZERO),
        executable_price: Some(Price::ZERO),
        executable_notional_usd: UsdNotional::ZERO,
        friction_cost_usd: Some(UsdNotional::ZERO),
        fee_usd: Some(UsdNotional::ZERO),
        rejection_reason: None,
        is_complete: true,
    }
}

fn selected_poly_book(
    context: &CanonicalPlanningContext,
    token_side: TokenSide,
) -> &OrderBookSnapshot {
    match token_side {
        TokenSide::Yes => &context.poly_yes_book,
        TokenSide::No => &context.poly_no_book,
    }
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < Decimal::ZERO {
        -value
    } else {
        value
    }
}

fn canonicalize_book(book: &OrderBookSnapshot, depth_levels: usize) -> OrderBookSnapshot {
    let mut canonical = book.clone();
    canonical.bids = canonicalize_levels(&book.bids, book.instrument, true, depth_levels);
    canonical.asks = canonicalize_levels(&book.asks, book.instrument, false, depth_levels);
    canonical
}

fn canonicalize_levels(
    levels: &[polyalpha_core::PriceLevel],
    instrument: InstrumentKind,
    descending: bool,
    depth_levels: usize,
) -> Vec<polyalpha_core::PriceLevel> {
    let mut aggregated = BTreeMap::new();
    for level in levels {
        let qty = match level.quantity {
            VenueQuantity::PolyShares(shares) => shares.0,
            VenueQuantity::CexBaseQty(qty) => qty.0,
        };
        if qty <= Decimal::ZERO {
            continue;
        }
        aggregated
            .entry(level.price)
            .and_modify(|total: &mut Decimal| *total += qty)
            .or_insert(qty);
    }

    let mut ordered: Vec<_> = aggregated.into_iter().collect();
    if descending {
        ordered.reverse();
    }
    ordered
        .into_iter()
        .take(depth_levels)
        .map(|(price, qty)| polyalpha_core::PriceLevel {
            price,
            quantity: match instrument {
                InstrumentKind::PolyYes | InstrumentKind::PolyNo => {
                    VenueQuantity::PolyShares(PolyShares(qty))
                }
                InstrumentKind::CexPerp => VenueQuantity::CexBaseQty(CexBaseQty(qty)),
            },
        })
        .collect()
}

fn planned_poly_notional(plan: &TradePlan) -> UsdNotional {
    if plan.poly_planned_shares.0 <= Decimal::ZERO || plan.poly_executable_price.0 <= Decimal::ZERO
    {
        UsdNotional::ZERO
    } else {
        plan.poly_planned_shares
            .to_usd_notional(plan.poly_executable_price)
    }
}

fn planned_cex_notional(plan: &TradePlan) -> UsdNotional {
    if plan.cex_planned_qty.0 <= Decimal::ZERO || plan.cex_executable_price.0 <= Decimal::ZERO {
        UsdNotional::ZERO
    } else {
        UsdNotional(plan.cex_planned_qty.0 * plan.cex_executable_price.0)
    }
}

fn scale_ratio(actual: UsdNotional, planned: UsdNotional) -> Decimal {
    if planned.0 <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        actual.0 / planned.0
    }
}

fn scale_usd(value: UsdNotional, scale: Decimal) -> UsdNotional {
    UsdNotional((value.0 * scale).max(Decimal::ZERO))
}

fn scale_f64(value: f64, scale: Decimal) -> f64 {
    let scaled = Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO) * scale;
    scaled.to_f64().unwrap_or_default()
}

#[derive(Clone, Copy, Debug)]
struct ReplannedEconomics {
    raw_edge_usd: UsdNotional,
    poly_friction_cost_usd: UsdNotional,
    cex_friction_cost_usd: UsdNotional,
    poly_fee_usd: UsdNotional,
    cex_fee_usd: UsdNotional,
    funding_cost_usd: UsdNotional,
    residual_risk_penalty_usd: UsdNotional,
    realized_edge_usd: UsdNotional,
    shock_loss_up_1pct: UsdNotional,
    shock_loss_down_1pct: UsdNotional,
    shock_loss_up_2pct: UsdNotional,
    shock_loss_down_2pct: UsdNotional,
}

fn scaled_economics_for_replan(
    reference: &TradePlan,
    planner: &ExecutionPlanner,
    poly_notional: UsdNotional,
    cex_notional: UsdNotional,
) -> ReplannedEconomics {
    let poly_scale = scale_ratio(poly_notional, planned_poly_notional(reference));
    let cex_scale = scale_ratio(cex_notional, planned_cex_notional(reference));
    let max_scale = poly_scale.max(cex_scale);
    let raw_edge_usd = scale_usd(reference.raw_edge_usd, poly_scale);
    let poly_friction_cost_usd = scale_usd(reference.poly_friction_cost_usd, poly_scale);
    let cex_friction_cost_usd = scale_usd(reference.cex_friction_cost_usd, cex_scale);
    let poly_fee_usd = if poly_notional.0 > Decimal::ZERO {
        bps_fee(poly_notional, planner.poly_fee_bps)
    } else {
        UsdNotional::ZERO
    };
    let cex_fee_usd = if cex_notional.0 > Decimal::ZERO {
        bps_fee(cex_notional, planner.cex_fee_bps(false))
    } else {
        UsdNotional::ZERO
    };
    let funding_cost_usd = if cex_notional.0 > Decimal::ZERO {
        bps_fee(cex_notional, planner.funding_bps_per_day())
    } else {
        UsdNotional::ZERO
    };
    let residual_risk_penalty_usd = scale_usd(reference.residual_risk_penalty_usd, max_scale);
    let realized_edge_usd = UsdNotional(
        raw_edge_usd.0
            - poly_friction_cost_usd.0
            - cex_friction_cost_usd.0
            - poly_fee_usd.0
            - cex_fee_usd.0
            - funding_cost_usd.0
            - residual_risk_penalty_usd.0,
    );

    ReplannedEconomics {
        raw_edge_usd,
        poly_friction_cost_usd,
        cex_friction_cost_usd,
        poly_fee_usd,
        cex_fee_usd,
        funding_cost_usd,
        residual_risk_penalty_usd,
        realized_edge_usd,
        shock_loss_up_1pct: scale_usd(reference.shock_loss_up_1pct, cex_scale),
        shock_loss_down_1pct: scale_usd(reference.shock_loss_down_1pct, cex_scale),
        shock_loss_up_2pct: scale_usd(reference.shock_loss_up_2pct, cex_scale),
        shock_loss_down_2pct: scale_usd(reference.shock_loss_down_2pct, cex_scale),
    }
}

fn bps_fee(notional: UsdNotional, bps: u32) -> UsdNotional {
    UsdNotional(notional.0 * Decimal::from(bps) / Decimal::from(10_000))
}

fn shock_losses_for_residual_delta(
    residual_delta: f64,
    reference_price: Price,
) -> [UsdNotional; 4] {
    let residual_qty = Decimal::from_f64_retain(residual_delta)
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO);
    let shock_base = UsdNotional(residual_qty * reference_price.0.max(Decimal::ZERO));
    [
        bps_fee(shock_base, 100),
        bps_fee(shock_base, 100),
        bps_fee(shock_base, 200),
        bps_fee(shock_base, 200),
    ]
}

fn best_level_price(book: &OrderBookSnapshot, side: OrderSide) -> Price {
    match side {
        OrderSide::Buy => book
            .asks
            .iter()
            .min_by(|left, right| left.price.cmp(&right.price))
            .map(|level| level.price)
            .unwrap_or(Price::ZERO),
        OrderSide::Sell => book
            .bids
            .iter()
            .max_by(|left, right| left.price.cmp(&right.price))
            .map(|level| level.price)
            .unwrap_or(Price::ZERO),
    }
}

fn mid_price(book: &OrderBookSnapshot) -> Option<Price> {
    let best_bid = book
        .bids
        .iter()
        .max_by(|left, right| left.price.cmp(&right.price))?;
    let best_ask = book
        .asks
        .iter()
        .min_by(|left, right| left.price.cmp(&right.price))?;
    Some(Price(
        (best_bid.price.0 + best_ask.price.0) / Decimal::from(2),
    ))
}

fn apply_linear_slippage(
    price: Price,
    side: OrderSide,
    bps: u64,
    clamp_probability: bool,
) -> Price {
    let multiplier = Decimal::from(bps) / Decimal::from(10_000);
    let raw = match side {
        OrderSide::Buy => price.0 * (Decimal::ONE + multiplier),
        OrderSide::Sell => price.0 * (Decimal::ONE - multiplier),
    };
    if clamp_probability {
        Price(raw.clamp(Decimal::ZERO, Decimal::ONE))
    } else {
        Price(raw)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{
        CexBaseQty, Exchange, Fill, InstrumentKind, OpenCandidate, OrderBookSnapshot, OrderId,
        OrderSide, PlanRejectionReason, PlanningIntent, PolyShares, Price, PriceLevel,
        ResidualSnapshot, RevalidationFailureReason, SignalStrength, Symbol, TokenSide,
        UsdNotional, VenueQuantity,
    };

    use super::{CanonicalPlanningContext, ExecutionPlanner};

    fn sample_open_intent() -> PlanningIntent {
        let candidate = OpenCandidate {
            schema_version: 1,
            candidate_id: "cand-plan-1".to_owned(),
            correlation_id: "corr-plan-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            token_side: TokenSide::Yes,
            direction: "long".to_owned(),
            fair_value: 0.69,
            raw_mispricing: 0.20,
            delta_estimate: 0.00012,
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
            max_shock_loss_usd: 200.0,
        }
    }

    fn sample_context() -> CanonicalPlanningContext {
        CanonicalPlanningContext {
            planner_depth_levels: 5,
            poly_yes_book: OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: Symbol::new("btc-price-only"),
                instrument: polyalpha_core::InstrumentKind::PolyYes,
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
                instrument: polyalpha_core::InstrumentKind::PolyNo,
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
                instrument: polyalpha_core::InstrumentKind::CexPerp,
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
            cex_qty_step: Decimal::new(1, 3),
            current_poly_yes_shares: Decimal::ZERO,
            current_poly_no_shares: Decimal::ZERO,
            current_cex_net_qty: Decimal::ZERO,
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
    fn planner_rejects_open_when_candidate_has_zero_cex_hedge_qty() {
        let planner = ExecutionPlanner::default();
        let mut intent = sample_open_intent();
        let PlanningIntent::OpenPosition { candidate, .. } = &mut intent else {
            panic!("expected open intent");
        };
        candidate.delta_estimate = 0.0;

        let rejection = planner.plan(&intent, &sample_context()).unwrap_err();

        assert_eq!(rejection.reason, PlanRejectionReason::ZeroCexHedgeQty);
        assert!(rejection.detail.contains("cannot produce"));
    }

    #[test]
    fn planner_rejects_open_when_cex_hedge_rounds_to_zero() {
        let planner = ExecutionPlanner::default();
        let mut intent = sample_open_intent();
        let PlanningIntent::OpenPosition { candidate, .. } = &mut intent else {
            panic!("expected open intent");
        };
        candidate.delta_estimate = 0.000001;

        let rejection = planner.plan(&intent, &sample_context()).unwrap_err();

        assert_eq!(rejection.reason, PlanRejectionReason::ZeroCexHedgeQty);
        assert!(rejection.detail.contains("step rounding"));
    }

    #[test]
    fn planner_planned_edge_includes_fees_and_funding() {
        let planner = ExecutionPlanner::default();
        let plan = planner
            .plan(&sample_open_intent(), &sample_context())
            .unwrap();

        assert!(plan.poly_fee_usd.0 > Decimal::ZERO);
        assert!(plan.cex_fee_usd.0 > Decimal::ZERO);
        assert!(plan.expected_funding_cost_usd.0 >= Decimal::ZERO);
        assert!(plan.planned_edge_usd.0 < plan.raw_edge_usd.0);
    }

    #[test]
    fn open_plan_uses_executable_price_as_buy_cap() {
        let planner = ExecutionPlanner::default();
        let plan = planner
            .plan(&sample_open_intent(), &sample_context())
            .unwrap();

        assert_eq!(plan.poly_max_avg_price, plan.poly_executable_price);
    }

    #[test]
    fn planner_shrinks_open_size_when_instant_loss_exceeds_budget_fraction() {
        let planner = ExecutionPlanner::default();
        let mut intent = sample_open_intent();
        let PlanningIntent::OpenPosition { candidate, .. } = &mut intent else {
            panic!("expected open intent");
        };
        candidate.risk_budget_usd = 200.0;

        let mut context = sample_context();
        context.poly_yes_book.bids = vec![PriceLevel {
            price: Price(Decimal::new(21, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(50, 0))),
        }];
        context.poly_yes_book.asks = vec![
            PriceLevel {
                price: Price(Decimal::new(21, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(50, 0))),
            },
            PriceLevel {
                price: Price(Decimal::new(25, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(5_000, 0))),
            },
        ];

        let plan = planner.plan(&intent, &context).unwrap();

        assert!(plan.poly_max_cost_usd.0 < Decimal::from(200u32));
        assert!(plan.poly_planned_shares.0 < Decimal::new(800, 0));
    }

    #[test]
    fn planner_searches_upward_when_flooring_initially_makes_budget_unexecutable() {
        let planner = ExecutionPlanner::default();
        let intent = sample_open_intent();
        let mut context = sample_context();
        context.cex_qty_step = Decimal::new(3, 2);
        context.poly_yes_book.bids = vec![PriceLevel {
            price: Price(Decimal::new(455, 3)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
        }];
        context.poly_yes_book.asks = vec![PriceLevel {
            price: Price(Decimal::new(485, 3)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(5_000, 0))),
        }];

        let plan = planner.plan(&intent, &context).unwrap();

        assert!(plan.poly_max_cost_usd.0 > Decimal::from(100u32));
        assert!(plan.poly_max_cost_usd.0 < Decimal::from(200u32));
        assert_eq!(plan.cex_planned_qty.0, Decimal::new(3, 2));
    }

    #[test]
    fn planner_preserves_non_positive_edge_after_loss_bounded_resize() {
        let planner = ExecutionPlanner::default();
        let intent = sample_open_intent();
        let mut context = sample_context();
        context.poly_yes_book.bids = vec![PriceLevel {
            price: Price(Decimal::new(10, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(1, 0))),
        }];
        context.poly_yes_book.asks = vec![PriceLevel {
            price: Price(Decimal::new(90, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
        }];

        let rejection = planner.plan(&intent, &context).unwrap_err();

        assert_eq!(rejection.reason, PlanRejectionReason::NonPositivePlannedEdge);
    }

    #[test]
    fn planner_rejects_open_when_every_executable_size_breaks_instant_loss_cap() {
        let planner = ExecutionPlanner::default();
        let intent = sample_open_intent();
        let mut context = sample_context();
        context.cex_qty_step = Decimal::new(1, 2);
        context.poly_yes_book.bids = vec![PriceLevel {
            price: Price(Decimal::new(10, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(1, 0))),
        }];
        context.poly_yes_book.asks = vec![PriceLevel {
            price: Price(Decimal::new(90, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
        }];

        let rejection = planner.plan(&intent, &context).unwrap_err();

        assert_eq!(rejection.reason, PlanRejectionReason::OpenInstantLossTooLarge);
    }

    #[test]
    fn close_plan_uses_existing_position_snapshot() {
        let planner = ExecutionPlanner::default();
        let mut context = sample_context();
        context.current_poly_yes_shares = Decimal::new(25, 0);
        context.current_cex_net_qty = Decimal::new(-3, 1);
        let intent = PlanningIntent::ClosePosition {
            schema_version: 1,
            intent_id: "intent-close-1".to_owned(),
            correlation_id: "corr-close-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            close_reason: "basis reverted".to_owned(),
            target_close_ratio: 1.0,
        };

        let plan = planner.plan(&intent, &context).unwrap();

        assert_eq!(plan.intent_type, "close_position");
        assert_eq!(plan.poly_sizing_mode, "sell_exact_shares");
        assert_eq!(plan.poly_token_side, TokenSide::Yes);
        assert_eq!(plan.poly_planned_shares, PolyShares(Decimal::new(25, 0)));
        assert_eq!(plan.cex_side, OrderSide::Buy);
        assert_eq!(plan.cex_planned_qty, CexBaseQty(Decimal::new(3, 1)));
        assert!(plan.poly_min_avg_price.0 > Decimal::ZERO);
    }

    #[test]
    fn delta_rebalance_prefers_cex_top_up_without_touching_poly() {
        let planner = ExecutionPlanner::default();
        let mut context = sample_context();
        context.current_poly_yes_shares = Decimal::new(800, 0);
        context.current_cex_net_qty = Decimal::new(-990, 2);

        let intent = PlanningIntent::DeltaRebalance {
            schema_version: 1,
            intent_id: "intent-delta-1".to_owned(),
            correlation_id: "corr-delta-1".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            residual_snapshot: ResidualSnapshot {
                schema_version: 1,
                residual_delta: 0.08,
                planned_cex_qty: CexBaseQty(Decimal::new(998, 2)),
                current_poly_yes_shares: PolyShares(Decimal::new(800, 0)),
                current_poly_no_shares: PolyShares::ZERO,
                preferred_cex_side: OrderSide::Sell,
            },
            target_residual_delta_max: 0.05,
            target_shock_loss_max: 50.0,
        };

        let plan = planner.plan(&intent, &context).unwrap();

        assert_eq!(plan.intent_type, "delta_rebalance");
        assert_eq!(plan.priority, "delta_rebalance");
        assert_eq!(plan.poly_planned_shares, PolyShares::ZERO);
        assert_eq!(plan.cex_side, OrderSide::Sell);
        assert!(plan.cex_planned_qty.0 > Decimal::ZERO);
    }

    #[test]
    fn delta_rebalance_rejects_tiny_untradeable_residual_without_flattening() {
        let planner = ExecutionPlanner::default();
        let mut context = sample_context();
        context.current_poly_yes_shares = Decimal::new(25, 0);
        context.current_cex_net_qty = Decimal::new(-3, 1);

        let intent = PlanningIntent::DeltaRebalance {
            schema_version: 1,
            intent_id: "intent-delta-2".to_owned(),
            correlation_id: "corr-delta-2".to_owned(),
            symbol: Symbol::new("btc-price-only"),
            residual_snapshot: ResidualSnapshot {
                schema_version: 1,
                residual_delta: 0.0004,
                planned_cex_qty: CexBaseQty(Decimal::new(25, 2)),
                current_poly_yes_shares: PolyShares(Decimal::new(25, 0)),
                current_poly_no_shares: PolyShares::ZERO,
                preferred_cex_side: OrderSide::Sell,
            },
            target_residual_delta_max: 0.05,
            target_shock_loss_max: 50.0,
        };

        let rejection = planner.plan(&intent, &context).unwrap_err();

        assert_eq!(rejection.reason, PlanRejectionReason::ResidualDeltaTooLarge);
        assert!(rejection.detail.contains("floors to zero"));
    }

    #[test]
    fn revalidate_rejects_when_poly_price_move_exceeds_limit() {
        let planner = ExecutionPlanner::default();
        let plan = planner
            .plan(&sample_open_intent(), &sample_context())
            .expect("plan should be created");
        let mut shifted = sample_context();
        shifted.poly_yes_book.asks = vec![PriceLevel {
            price: Price(plan.poly_executable_price.0 + Decimal::new(5, 2)),
            quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
        }];

        let failure = planner
            .revalidate(&plan, &shifted)
            .expect_err("poly move should invalidate the plan");

        assert_eq!(failure, RevalidationFailureReason::PolyPriceMoveExceeded);
    }

    #[test]
    fn revalidate_rejects_when_planned_edge_deteriorates() {
        let planner = ExecutionPlanner::default();
        let mut plan = planner
            .plan(&sample_open_intent(), &sample_context())
            .expect("plan should be created");
        plan.min_planned_edge_usd = UsdNotional(plan.planned_edge_usd.0 + Decimal::ONE);
        let mut shifted = sample_context();
        shifted.cex_book.asks = vec![PriceLevel {
            price: Price(plan.cex_executable_price.0 + Decimal::new(40, 0)),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(5, 0))),
        }];

        let failure = planner
            .revalidate(&plan, &shifted)
            .expect_err("higher hedge cost should invalidate planned edge");

        assert_eq!(failure, RevalidationFailureReason::PlannedEdgeDeteriorated);
    }

    #[test]
    fn revalidate_rejects_when_residual_risk_deteriorates() {
        let planner = ExecutionPlanner::default();
        let mut plan = planner
            .plan(&sample_open_intent(), &sample_context())
            .expect("plan should be created");
        plan.post_rounding_residual_delta = 0.5;
        plan.max_shock_loss_usd = polyalpha_core::UsdNotional(Decimal::new(1, 2));

        let failure = planner
            .revalidate(&plan, &sample_context())
            .expect_err("oversized residual risk should invalidate the plan");

        assert_eq!(failure, RevalidationFailureReason::ResidualRiskDeteriorated);
    }

    #[test]
    fn replan_from_execution_rescales_child_hedge_to_actual_fill() {
        let planner = ExecutionPlanner::default();
        let parent = planner
            .plan(&sample_open_intent(), &sample_context())
            .expect("parent plan should be created");
        let actual_fill = Fill {
            fill_id: "fill-poly-1".to_owned(),
            correlation_id: parent.correlation_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: parent.symbol.clone(),
            instrument: InstrumentKind::PolyYes,
            order_id: OrderId("poly-order-1".to_owned()),
            side: OrderSide::Buy,
            price: parent.poly_executable_price,
            quantity: VenueQuantity::PolyShares(PolyShares(
                parent.poly_planned_shares.0 / Decimal::TWO,
            )),
            notional_usd: polyalpha_core::UsdNotional(parent.poly_max_cost_usd.0 / Decimal::TWO),
            fee: polyalpha_core::UsdNotional::ZERO,
            is_maker: false,
            timestamp_ms: parent.created_at_ms + 1,
        };

        let child = planner
            .replan_from_execution(&parent, &[actual_fill], &sample_context())
            .expect("child hedge replan should succeed");

        assert!(child.cex_planned_qty.0 > Decimal::ZERO);
        assert!(child.cex_planned_qty.0 < parent.cex_planned_qty.0);
        assert_eq!(
            child.poly_planned_shares,
            PolyShares(parent.poly_planned_shares.0 / Decimal::TWO)
        );
    }
}
