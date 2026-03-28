use std::sync::Arc;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use polyalpha_core::{
    CexBaseQty, OpenCandidate, OrderBookSnapshot, OrderRequest, OrderSide, PlanRejectionReason,
    PlanningIntent, PolyOrderRequest, PolyShares, PolySizingInstruction, Price,
    RevalidationFailureReason, Symbol, TokenSide, TradePlan, UsdNotional, PLANNING_SCHEMA_VERSION,
};

use crate::{
    dry_run::{DryRunExecutor, OrderExecutionEstimate, SlippageConfig},
    orderbook_provider::InMemoryOrderbookProvider,
};

const DEFAULT_PLAN_TTL_MS: u64 = 1_000;
const DEFAULT_MAX_SEQUENCE_DRIFT: u64 = 2;
const DEFAULT_POLY_SLIPPAGE_BPS: u64 = 50;
const DEFAULT_CEX_SLIPPAGE_BPS: u64 = 2;

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

impl CanonicalPlanningContext {
    pub fn canonical_bytes(&self, intent: &PlanningIntent) -> Vec<u8> {
        serde_json::to_vec(&(intent, self)).expect("canonical planning context")
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionPlanner {
    pub poly_fee_bps: u32,
    pub cex_fee_bps: u32,
    pub funding_bps_per_day: u32,
    pub poly_slippage_bps: u32,
    pub cex_slippage_bps: u32,
}

impl Default for ExecutionPlanner {
    fn default() -> Self {
        Self {
            poly_fee_bps: 20,
            cex_fee_bps: 5,
            funding_bps_per_day: 1,
            poly_slippage_bps: DEFAULT_POLY_SLIPPAGE_BPS as u32,
            cex_slippage_bps: DEFAULT_CEX_SLIPPAGE_BPS as u32,
        }
    }
}

impl ExecutionPlanner {
    pub fn plan(
        &self,
        intent: &PlanningIntent,
        context: &CanonicalPlanningContext,
    ) -> Result<TradePlan, PlanRejection> {
        self.validate_context(context)?;
        match intent {
            PlanningIntent::OpenPosition { .. } => self.plan_open(intent, context),
            PlanningIntent::ClosePosition { .. } => {
                self.plan_flatten(intent, context, "close_position", "sell_exact_shares")
            }
            PlanningIntent::DeltaRebalance { .. } => {
                self.plan_flatten(intent, context, "delta_rebalance", "sell_exact_shares")
            }
            PlanningIntent::ResidualRecovery { .. } => {
                self.plan_flatten(intent, context, "residual_recovery", "sell_exact_shares")
            }
            PlanningIntent::ForceExit { .. } => {
                self.plan_flatten(intent, context, "force_exit", "sell_exact_shares")
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
        Ok(())
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
        let cex_planned_qty = exact_cex_qty.floor_to_step(context.cex_qty_step);
        let post_rounding_residual_qty = (exact_cex_qty.0 - cex_planned_qty.0).max(Decimal::ZERO);
        if exact_cex_qty.0 > Decimal::ZERO && cex_planned_qty.0 <= Decimal::ZERO {
            return Err(PlanRejection {
                reason: PlanRejectionReason::ResidualDeltaTooLarge,
                detail: "rounded hedge qty floors to zero".to_owned(),
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
        let raw_edge_usd = poly_estimate.executable_notional_usd.0
            * Decimal::from_f64(candidate.raw_mispricing.abs()).unwrap_or(Decimal::ZERO);
        let hedge_notional_proxy = cex_estimate
            .executable_notional_usd
            .0
            .max(poly_estimate.executable_notional_usd.0 * delta_abs);
        let poly_fee_usd = bps_fee(poly_estimate.executable_notional_usd, self.poly_fee_bps);
        let cex_fee_usd = bps_fee(UsdNotional(hedge_notional_proxy), self.cex_fee_bps);
        let expected_funding_cost_usd =
            bps_fee(UsdNotional(hedge_notional_proxy), self.funding_bps_per_day);
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
            poly_max_avg_price: Price::ONE,
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
        let cex_fee_usd = bps_fee(cex_estimate.executable_notional_usd, self.cex_fee_bps);
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
            fee_usd: Some(bps_fee(executable_notional_usd, self.cex_fee_bps)),
            rejection_reason: if remaining <= Decimal::ZERO {
                None
            } else {
                Some("insufficient_cex_depth".to_owned())
            },
            is_complete: remaining <= Decimal::ZERO,
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

fn bps_fee(notional: UsdNotional, bps: u32) -> UsdNotional {
    UsdNotional(notional.0 * Decimal::from(bps) / Decimal::from(10_000))
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
        CexBaseQty, Exchange, OpenCandidate, OrderBookSnapshot, OrderSide, PlanRejectionReason,
        PlanningIntent, PolyShares, Price, PriceLevel, SignalStrength, Symbol, TokenSide,
        VenueQuantity,
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
}
