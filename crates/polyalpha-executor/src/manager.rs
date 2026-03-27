use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use uuid::Uuid;

use polyalpha_core::{
    cex_venue_symbol, ArbSignalAction, ArbSignalEvent, CexOrderRequest, ClientOrderId, CoreError,
    DmmQuoteState, DmmQuoteUpdate, Exchange, ExecutionEvent, ExecutionResult, Fill, HedgeState,
    InstrumentKind, OrderExecutor, OrderId, OrderLedgerEntry, OrderRequest, OrderResponse,
    OrderSide, OrderStatus, OrderType, PlanningIntent, PolyOrderRequest, PolyShares,
    PolySizingInstruction, PositionTracker, Price, RecoveryDecisionReason, Result, Symbol,
    SymbolRegistry, TimeInForce, TokenSide, TradePlan, UsdNotional, VenueQuantity,
    PLANNING_SCHEMA_VERSION,
};

use crate::{
    orderbook_provider::OrderbookProvider,
    plan_state::{priority_for_intent, priority_rank, InFlightPlan, PlanLifecycleState},
    planner::{CanonicalPlanningContext, ExecutionPlanner},
};

#[derive(Clone, Debug)]
struct ActiveDmmOrders {
    bid_order_id: OrderId,
    ask_order_id: OrderId,
}

#[derive(Clone, Debug)]
struct SessionBookkeeping {
    session_id: Uuid,
    state: HedgeState,
    last_signal_id: Option<String>,
    correlation_id: Option<String>,
    realized_pnl: UsdNotional,
}

impl SessionBookkeeping {
    fn new() -> Self {
        Self {
            session_id: Uuid::new_v4(),
            state: HedgeState::Idle,
            last_signal_id: None,
            correlation_id: None,
            realized_pnl: UsdNotional::ZERO,
        }
    }
}

#[derive(Clone)]
pub struct ExecutionManager<E: OrderExecutor> {
    executor: E,
    symbol_registry: Option<SymbolRegistry>,
    orderbook_provider: Option<Arc<dyn OrderbookProvider>>,
    planner: ExecutionPlanner,
    dmm_quote_states: HashMap<Symbol, DmmQuoteState>,
    active_dmm_orders: HashMap<Symbol, ActiveDmmOrders>,
    active_plans: HashMap<Symbol, InFlightPlan>,
    position_tracker: PositionTracker,
    sessions: HashMap<Symbol, SessionBookkeeping>,
    next_client_order_seq: u64,
    next_fill_seq: u64,
}

impl<E: OrderExecutor> ExecutionManager<E> {
    pub fn new(executor: E) -> Self {
        Self::with_parts(executor, None, None)
    }

    pub fn with_symbol_registry(executor: E, symbol_registry: SymbolRegistry) -> Self {
        Self::with_parts(executor, Some(symbol_registry), None)
    }

    pub fn with_orderbook_provider(
        executor: E,
        orderbook_provider: Arc<dyn OrderbookProvider>,
    ) -> Self {
        Self::with_parts(executor, None, Some(orderbook_provider))
    }

    pub fn with_symbol_registry_and_orderbook_provider(
        executor: E,
        symbol_registry: SymbolRegistry,
        orderbook_provider: Arc<dyn OrderbookProvider>,
    ) -> Self {
        Self::with_parts(executor, Some(symbol_registry), Some(orderbook_provider))
    }

    fn with_parts(
        executor: E,
        symbol_registry: Option<SymbolRegistry>,
        orderbook_provider: Option<Arc<dyn OrderbookProvider>>,
    ) -> Self {
        Self {
            executor,
            symbol_registry,
            orderbook_provider,
            planner: ExecutionPlanner::default(),
            dmm_quote_states: HashMap::new(),
            active_dmm_orders: HashMap::new(),
            active_plans: HashMap::new(),
            position_tracker: PositionTracker::default(),
            sessions: HashMap::new(),
            next_client_order_seq: 1,
            next_fill_seq: 1,
        }
    }

    pub fn dmm_quote_state(&self, symbol: &Symbol) -> Option<&DmmQuoteState> {
        self.dmm_quote_states.get(symbol)
    }

    pub fn planner(&self) -> &ExecutionPlanner {
        &self.planner
    }

    pub fn orderbook_provider(&self) -> Option<&Arc<dyn OrderbookProvider>> {
        self.orderbook_provider.as_ref()
    }

    pub fn hedge_state(&self, symbol: &Symbol) -> Option<HedgeState> {
        self.sessions.get(symbol).map(|session| session.state)
    }

    pub fn last_signal_id(&self, symbol: &Symbol) -> Option<&str> {
        self.sessions
            .get(symbol)
            .and_then(|session| session.last_signal_id.as_deref())
    }

    pub fn active_dmm_order_count(&self, symbol: &Symbol) -> usize {
        if self.active_dmm_orders.contains_key(symbol) {
            2
        } else {
            0
        }
    }

    pub async fn apply_dmm_quote_update(
        &mut self,
        update: DmmQuoteUpdate,
    ) -> Result<Vec<ExecutionEvent>> {
        let symbol = update.symbol.clone();
        let mut events = Vec::new();
        self.cancel_active_quotes(&symbol, &mut events).await?;

        match update.next_state {
            Some(state) => {
                self.dmm_quote_states.insert(symbol.clone(), state.clone());

                let bid_request = self.build_dmm_order_request(&state, OrderSide::Buy);
                let ask_request = self.build_dmm_order_request(&state, OrderSide::Sell);

                let bid_response = self
                    .submit_order_with_events(
                        symbol.clone(),
                        Exchange::Polymarket,
                        bid_request,
                        None,
                        &mut events,
                    )
                    .await?;
                let ask_response = self
                    .submit_order_with_events(
                        symbol.clone(),
                        Exchange::Polymarket,
                        ask_request,
                        None,
                        &mut events,
                    )
                    .await?;

                let maybe_bid = if matches!(bid_response.status, OrderStatus::Open) {
                    Some(bid_response.exchange_order_id)
                } else {
                    None
                };
                let maybe_ask = if matches!(ask_response.status, OrderStatus::Open) {
                    Some(ask_response.exchange_order_id)
                } else {
                    None
                };
                if let (Some(bid_order_id), Some(ask_order_id)) = (maybe_bid, maybe_ask) {
                    self.active_dmm_orders.insert(
                        symbol,
                        ActiveDmmOrders {
                            bid_order_id,
                            ask_order_id,
                        },
                    );
                }
            }
            None => {
                self.dmm_quote_states.remove(&symbol);
            }
        }

        Ok(events)
    }

    pub async fn process_arb_signal(
        &mut self,
        signal: ArbSignalEvent,
    ) -> Result<Vec<ExecutionEvent>> {
        if let Some(reason) = self.invalid_open_signal_reason(&signal) {
            return Ok(vec![self.rejected_signal_event(&signal, reason)]);
        }

        let mut events = Vec::new();
        let symbol = signal.symbol.clone();
        let correlation_id = signal.correlation_id.clone();
        let (session_id, mut current_state) = self.session_identity(&symbol);

        // Store correlation_id in session for tracking
        if let Some(session) = self.sessions.get_mut(&symbol) {
            session.correlation_id = Some(correlation_id.clone());
        }

        let is_close = matches!(signal.action, ArbSignalAction::ClosePosition { .. });
        let position_before = !self.position_tracker.symbol_is_flat(&symbol);

        let next_state = match &signal.action {
            ArbSignalAction::BasisLong { .. }
            | ArbSignalAction::BasisShort { .. }
            | ArbSignalAction::NegRiskArb { .. } => HedgeState::SubmittingLegs,
            ArbSignalAction::DeltaRebalance { .. } => HedgeState::Rebalancing,
            ArbSignalAction::ClosePosition { .. } => HedgeState::Closing,
        };
        if current_state != next_state {
            events.push(ExecutionEvent::HedgeStateChanged {
                symbol: symbol.clone(),
                session_id,
                old_state: current_state,
                new_state: next_state,
                timestamp_ms: signal.timestamp_ms,
            });
            current_state = next_state;
            self.update_session_state(&symbol, next_state);
        }

        let requests = self.requests_from_signal(&signal);
        let mut all_requests_filled = true;
        for request in requests {
            let exchange = match &request {
                OrderRequest::Poly(_) => Exchange::Polymarket,
                OrderRequest::Cex(req) => req.exchange,
            };
            let response = self
                .submit_order_with_events(
                    symbol.clone(),
                    exchange,
                    request.clone(),
                    Some(&correlation_id),
                    &mut events,
                )
                .await?;
            if matches!(response.status, OrderStatus::Filled) {
                let fill = self.fill_from_request_and_response(request, response, &correlation_id);
                self.apply_fill_to_position(&fill);
                events.push(ExecutionEvent::OrderFilled(fill));
            } else {
                all_requests_filled = false;
            }
        }

        let position_cleared = position_before && self.position_tracker.symbol_is_flat(&symbol);

        // Check for single-leg failure scenario
        if !all_requests_filled
            && !is_close
            && events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::OrderFilled(_)))
        {
            events.push(ExecutionEvent::ReconcileRequired {
                symbol: Some(symbol.clone()),
                reason: "partial fill - single leg executed".to_owned(),
            });
        }

        let final_state = if is_close {
            if all_requests_filled && position_cleared {
                // Generate TradeClosed event when position is fully closed
                let realized_pnl = if let Some(session) = self.sessions.get_mut(&symbol) {
                    let pnl = session.realized_pnl;
                    session.realized_pnl = UsdNotional::ZERO;
                    session.correlation_id = None;
                    pnl
                } else {
                    UsdNotional::ZERO
                };
                events.push(ExecutionEvent::TradeClosed {
                    symbol: symbol.clone(),
                    correlation_id: correlation_id.clone(),
                    realized_pnl,
                    timestamp_ms: signal.timestamp_ms,
                });
                HedgeState::Idle
            } else {
                HedgeState::Closing
            }
        } else if all_requests_filled {
            HedgeState::Hedged
        } else {
            current_state
        };
        if current_state != final_state {
            events.push(ExecutionEvent::HedgeStateChanged {
                symbol,
                session_id,
                old_state: current_state,
                new_state: final_state,
                timestamp_ms: signal.timestamp_ms,
            });
            self.update_session_state(&signal.symbol, final_state);
        }
        self.update_last_signal(&signal.symbol, signal.signal_id);

        Ok(events)
    }

    pub async fn process_intent(&mut self, intent: PlanningIntent) -> Result<Vec<ExecutionEvent>> {
        let context = self.build_planning_context(&intent)?;
        let mut plan = self.planner.plan(&intent, &context).map_err(|rejection| {
            CoreError::Generic(format!(
                "plan rejected [{}]: {}",
                rejection.reason.code(),
                rejection.detail
            ))
        })?;

        let mut events = Vec::new();
        if let Some(existing) = self.active_plans.get(intent.symbol()).cloned() {
            let incoming_rank = priority_rank(priority_for_intent(&intent));
            if existing.idempotency_key == plan.idempotency_key {
                return Ok(events);
            }
            if incoming_rank > existing.priority_rank() {
                plan.supersedes_plan_id = Some(existing.plan.plan_id.clone());
                events.push(ExecutionEvent::PlanSuperseded {
                    symbol: intent.symbol().clone(),
                    superseded_plan_id: existing.plan.plan_id.clone(),
                    next_plan_id: plan.plan_id.clone(),
                });
            } else {
                return Err(CoreError::Generic(format!(
                    "higher priority plan active for {}",
                    intent.symbol().0
                )));
            }
        }

        self.install_plan(plan.clone(), PlanLifecycleState::PlanReady);
        events.push(ExecutionEvent::TradePlanCreated { plan: plan.clone() });

        if self.plan_is_noop(&plan) {
            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
            return Ok(events);
        }

        events.extend(self.process_plan(plan).await?);
        Ok(events)
    }

    pub async fn process_plan(&mut self, plan: TradePlan) -> Result<Vec<ExecutionEvent>> {
        self.install_plan(plan.clone(), PlanLifecycleState::SubmittingPoly);
        if plan.poly_planned_shares.0 > Decimal::ZERO {
            self.submit_poly_leg(&plan).await
        } else if plan.cex_planned_qty.0 > Decimal::ZERO {
            self.submit_cex_leg(&plan).await
        } else {
            Ok(Vec::new())
        }
    }

    fn build_planning_context(&self, intent: &PlanningIntent) -> Result<CanonicalPlanningContext> {
        let provider = self.orderbook_provider.as_ref().ok_or_else(|| {
            CoreError::Generic("planning orderbook provider is unavailable".to_owned())
        })?;
        let symbol = intent.symbol();
        let (cex_exchange, venue_symbol) = self.resolve_cex_target(symbol);
        let poly_yes_book = provider
            .get_orderbook(Exchange::Polymarket, symbol, InstrumentKind::PolyYes)
            .unwrap_or_else(|| {
                empty_orderbook(
                    Exchange::Polymarket,
                    symbol.clone(),
                    InstrumentKind::PolyYes,
                )
            });
        let poly_no_book = provider
            .get_orderbook(Exchange::Polymarket, symbol, InstrumentKind::PolyNo)
            .unwrap_or_else(|| {
                empty_orderbook(Exchange::Polymarket, symbol.clone(), InstrumentKind::PolyNo)
            });
        let cex_book = provider
            .get_cex_orderbook(cex_exchange, &venue_symbol)
            .unwrap_or_else(|| {
                empty_orderbook(cex_exchange, symbol.clone(), InstrumentKind::CexPerp)
            });
        let now_ms = poly_yes_book
            .received_at_ms
            .max(poly_no_book.received_at_ms)
            .max(cex_book.received_at_ms);

        Ok(CanonicalPlanningContext {
            planner_depth_levels: 1,
            poly_yes_book,
            poly_no_book,
            cex_book,
            now_ms,
        })
    }

    fn plan_is_noop(&self, plan: &TradePlan) -> bool {
        plan.poly_planned_shares.0 <= Decimal::ZERO && plan.cex_planned_qty.0 <= Decimal::ZERO
    }

    fn install_plan(&mut self, plan: TradePlan, state: PlanLifecycleState) {
        let symbol = plan.symbol.clone();
        let mut in_flight = InFlightPlan::new(plan);
        in_flight.state = state;
        self.active_plans.insert(symbol, in_flight);
    }

    fn update_plan_state(&mut self, symbol: &Symbol, state: PlanLifecycleState) {
        if let Some(active) = self.active_plans.get_mut(symbol) {
            active.state = state;
        }
    }

    async fn submit_poly_leg(&mut self, plan: &TradePlan) -> Result<Vec<ExecutionEvent>> {
        let request = self.poly_request_from_plan(plan);
        let mut events = Vec::new();
        let response = self
            .submit_order_with_events(
                plan.symbol.clone(),
                Exchange::Polymarket,
                request.clone(),
                Some(&plan.correlation_id),
                &mut events,
            )
            .await?;

        if matches!(
            response.status,
            OrderStatus::Filled | OrderStatus::PartialFill
        ) && has_filled_quantity(&response.filled_quantity)
        {
            let fill = self.fill_from_request_and_response(
                request,
                response.clone(),
                &plan.correlation_id,
            );
            self.apply_fill_to_position(&fill);
            events.push(ExecutionEvent::OrderFilled(fill.clone()));
            events.extend(self.on_poly_fill(plan, &fill).await?);
        } else {
            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
        }

        Ok(events)
    }

    async fn on_poly_fill(&mut self, plan: &TradePlan, fill: &Fill) -> Result<Vec<ExecutionEvent>> {
        let mut events = Vec::new();
        let Some(child_plan) = self.child_hedge_plan_from_fill(plan, fill)? else {
            self.update_plan_state(&plan.symbol, PlanLifecycleState::VerifyingResidual);
            let result = self.execution_result_for_plan(plan, None, None, fill.notional_usd, 0.0);
            if let Some(recovery_plan) = self.recovery_plan_from_result(plan, &result)? {
                self.install_plan(recovery_plan.clone(), PlanLifecycleState::Recovering);
                events.push(ExecutionEvent::RecoveryPlanCreated {
                    plan: recovery_plan,
                });
            } else {
                self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
            }
            return Ok(events);
        };

        self.install_plan(child_plan.clone(), PlanLifecycleState::HedgingCex);
        events.push(ExecutionEvent::TradePlanCreated {
            plan: child_plan.clone(),
        });
        let submit_events = self.submit_cex_leg(&child_plan).await?;
        events.extend(submit_events);
        Ok(events)
    }

    async fn submit_cex_leg(&mut self, plan: &TradePlan) -> Result<Vec<ExecutionEvent>> {
        let request = self.cex_request_from_plan(plan);
        let mut events = Vec::new();
        let response = self
            .submit_order_with_events(
                plan.symbol.clone(),
                match &request {
                    OrderRequest::Cex(req) => req.exchange,
                    OrderRequest::Poly(_) => Exchange::Polymarket,
                },
                request.clone(),
                Some(&plan.correlation_id),
                &mut events,
            )
            .await?;

        let mut cex_fill = None;
        if matches!(
            response.status,
            OrderStatus::Filled | OrderStatus::PartialFill
        ) && has_filled_quantity(&response.filled_quantity)
        {
            let fill = self.fill_from_request_and_response(
                request.clone(),
                response.clone(),
                &plan.correlation_id,
            );
            self.apply_fill_to_position(&fill);
            events.push(ExecutionEvent::OrderFilled(fill.clone()));
            cex_fill = Some(fill);
        }

        self.update_plan_state(&plan.symbol, PlanLifecycleState::VerifyingResidual);
        let residual_delta = residual_cex_delta(plan, cex_fill.as_ref());
        let result = self.execution_result_for_plan(
            plan,
            Some(&request),
            Some(&response),
            UsdNotional::ZERO,
            residual_delta,
        );
        if let Some(recovery_plan) = self.recovery_plan_from_result(plan, &result)? {
            self.install_plan(recovery_plan.clone(), PlanLifecycleState::Recovering);
            events.push(ExecutionEvent::RecoveryPlanCreated {
                plan: recovery_plan,
            });
        } else {
            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
        }

        Ok(events)
    }

    fn poly_request_from_plan(&mut self, plan: &TradePlan) -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: self.next_client_order_id("plan-poly"),
            symbol: plan.symbol.clone(),
            token_side: plan.poly_token_side,
            side: plan.poly_side,
            order_type: OrderType::Market,
            sizing: match plan.poly_sizing_mode.as_str() {
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
            },
            time_in_force: TimeInForce::Fok,
            post_only: false,
        })
    }

    fn cex_request_from_plan(&mut self, plan: &TradePlan) -> OrderRequest {
        self.cex_market_order_with_options(
            plan.symbol.clone(),
            plan.cex_side,
            plan.cex_planned_qty,
            matches!(
                plan.intent_type.as_str(),
                "close_position" | "force_exit" | "residual_recovery"
            ),
        )
    }

    fn child_hedge_plan_from_fill(
        &self,
        parent_plan: &TradePlan,
        fill: &Fill,
    ) -> Result<Option<TradePlan>> {
        if parent_plan.cex_planned_qty.0 <= Decimal::ZERO
            || parent_plan.poly_planned_shares.0 <= Decimal::ZERO
        {
            return Ok(None);
        }

        let VenueQuantity::PolyShares(actual_poly_shares) = fill.quantity else {
            return Ok(None);
        };
        if actual_poly_shares.0 <= Decimal::ZERO {
            return Ok(None);
        }

        let requested_qty = if parent_plan.poly_planned_shares.0 > Decimal::ZERO {
            polyalpha_core::CexBaseQty(
                parent_plan.cex_planned_qty.0 * actual_poly_shares.0
                    / parent_plan.poly_planned_shares.0,
            )
        } else {
            polyalpha_core::CexBaseQty::ZERO
        };
        let normalized_qty = self.normalize_cex_qty(&parent_plan.symbol, requested_qty);
        if normalized_qty.0 <= Decimal::ZERO {
            return Ok(None);
        }

        let context = self.build_planning_context(&PlanningIntent::DeltaRebalance {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: format!("hedge-context-{}", parent_plan.plan_id),
            correlation_id: parent_plan.correlation_id.clone(),
            symbol: parent_plan.symbol.clone(),
            target_residual_delta_max: parent_plan.max_residual_delta,
            target_shock_loss_max: parent_plan
                .max_shock_loss_usd
                .0
                .to_f64()
                .unwrap_or_default(),
        })?;
        let plan_hash = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{}", parent_plan.plan_hash, fill.fill_id).as_bytes(),
        )
        .to_string();

        let mut child_plan = parent_plan.clone();
        child_plan.plan_id = format!("plan-{plan_hash}");
        child_plan.parent_plan_id = Some(parent_plan.plan_id.clone());
        child_plan.supersedes_plan_id = None;
        child_plan.idempotency_key = format!("{}:{}", parent_plan.idempotency_key, fill.fill_id);
        child_plan.plan_hash = plan_hash;
        child_plan.intent_type = "child_hedge".to_owned();
        child_plan.priority = "delta_rebalance".to_owned();
        child_plan.created_at_ms = fill.timestamp_ms;
        child_plan.poly_requested_shares = actual_poly_shares;
        child_plan.poly_planned_shares = actual_poly_shares;
        child_plan.poly_max_shares = actual_poly_shares;
        child_plan.poly_max_cost_usd = fill.notional_usd;
        child_plan.poly_executable_price = fill.price;
        child_plan.cex_exchange_timestamp_ms = context.cex_book.exchange_timestamp_ms;
        child_plan.cex_received_at_ms = context.cex_book.received_at_ms;
        child_plan.cex_sequence = context.cex_book.sequence;
        child_plan.cex_book_avg_price = best_bid_or_ask(&context.cex_book, child_plan.cex_side);
        child_plan.cex_executable_price = child_plan.cex_book_avg_price;
        child_plan.cex_planned_qty = normalized_qty;
        Ok(Some(child_plan))
    }

    fn execution_result_for_plan(
        &self,
        plan: &TradePlan,
        request: Option<&OrderRequest>,
        response: Option<&OrderResponse>,
        actual_poly_cost_usd: UsdNotional,
        actual_residual_delta: f64,
    ) -> ExecutionResult {
        let ledger_entry = match (request, response) {
            (Some(request), Some(response)) => vec![order_ledger_entry(request, response)],
            _ => Vec::new(),
        };
        ExecutionResult {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: plan.plan_id.clone(),
            correlation_id: plan.correlation_id.clone(),
            symbol: plan.symbol.clone(),
            status: if response
                .map(|item| matches!(item.status, OrderStatus::Filled | OrderStatus::PartialFill))
                .unwrap_or(false)
            {
                "completed".to_owned()
            } else {
                "degraded".to_owned()
            },
            poly_order_ledger: Vec::new(),
            cex_order_ledger: ledger_entry,
            actual_poly_cost_usd,
            actual_cex_cost_usd: response
                .and_then(|item| {
                    item.average_price
                        .map(|price| response_notional(item, price))
                })
                .unwrap_or(UsdNotional::ZERO),
            actual_poly_fee_usd: UsdNotional::ZERO,
            actual_cex_fee_usd: UsdNotional::ZERO,
            actual_funding_cost_usd: UsdNotional::ZERO,
            realized_edge_usd: plan.planned_edge_usd,
            plan_vs_fill_deviation_usd: UsdNotional::ZERO,
            actual_residual_delta,
            actual_shock_loss_up_1pct: UsdNotional::ZERO,
            actual_shock_loss_down_1pct: UsdNotional::ZERO,
            actual_shock_loss_up_2pct: UsdNotional::ZERO,
            actual_shock_loss_down_2pct: UsdNotional::ZERO,
            recovery_required: actual_residual_delta > plan.max_residual_delta,
            timestamp_ms: response
                .map(|item| item.timestamp_ms)
                .unwrap_or(plan.created_at_ms),
        }
    }

    fn recovery_plan_from_result(
        &self,
        plan: &TradePlan,
        result: &ExecutionResult,
    ) -> Result<Option<TradePlan>> {
        let Some(intent) = self.recovery_intent_from_result(plan, result) else {
            return Ok(None);
        };
        let context = self.build_planning_context(&intent)?;
        let mut recovery_plan = self.planner.plan(&intent, &context).map_err(|rejection| {
            CoreError::Generic(format!(
                "recovery plan rejected [{}]: {}",
                rejection.reason.code(),
                rejection.detail
            ))
        })?;
        recovery_plan.parent_plan_id = Some(result.plan_id.clone());
        recovery_plan.supersedes_plan_id = Some(plan.plan_id.clone());
        Ok(Some(recovery_plan))
    }

    fn recovery_intent_from_result(
        &self,
        plan: &TradePlan,
        result: &ExecutionResult,
    ) -> Option<PlanningIntent> {
        if result.actual_residual_delta <= plan.max_residual_delta {
            return None;
        }

        Some(PlanningIntent::ResidualRecovery {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: format!("recover-{}", result.plan_id),
            correlation_id: result.correlation_id.clone(),
            symbol: result.symbol.clone(),
            residual_snapshot: format!(
                "residual_delta={:.6};planned_qty={}",
                result.actual_residual_delta, plan.cex_planned_qty.0
            ),
            recovery_reason: RecoveryDecisionReason::ForceExitRequired,
        })
    }

    async fn cancel_active_quotes(
        &mut self,
        symbol: &Symbol,
        out: &mut Vec<ExecutionEvent>,
    ) -> Result<()> {
        let Some(active) = self.active_dmm_orders.remove(symbol) else {
            return Ok(());
        };

        self.executor
            .cancel_order(Exchange::Polymarket, &active.bid_order_id)
            .await?;
        out.push(ExecutionEvent::OrderCancelled {
            symbol: symbol.clone(),
            order_id: active.bid_order_id,
            exchange: Exchange::Polymarket,
        });

        self.executor
            .cancel_order(Exchange::Polymarket, &active.ask_order_id)
            .await?;
        out.push(ExecutionEvent::OrderCancelled {
            symbol: symbol.clone(),
            order_id: active.ask_order_id,
            exchange: Exchange::Polymarket,
        });
        Ok(())
    }

    async fn submit_order_with_events(
        &self,
        symbol: Symbol,
        exchange: Exchange,
        request: OrderRequest,
        correlation_id: Option<&str>,
        out: &mut Vec<ExecutionEvent>,
    ) -> Result<OrderResponse> {
        let response = self.executor.submit_order(request).await?;
        out.push(ExecutionEvent::OrderSubmitted {
            symbol,
            exchange,
            response: response.clone(),
            correlation_id: correlation_id.map(|s| s.to_owned()).unwrap_or_default(),
        });
        Ok(response)
    }

    fn session_identity(&mut self, symbol: &Symbol) -> (Uuid, HedgeState) {
        let session = self
            .sessions
            .entry(symbol.clone())
            .or_insert_with(SessionBookkeeping::new);
        (session.session_id, session.state)
    }

    fn update_session_state(&mut self, symbol: &Symbol, state: HedgeState) {
        if let Some(session) = self.sessions.get_mut(symbol) {
            session.state = state;
        }
    }

    fn update_last_signal(&mut self, symbol: &Symbol, signal_id: String) {
        if let Some(session) = self.sessions.get_mut(symbol) {
            session.last_signal_id = Some(signal_id);
        }
    }

    fn build_dmm_order_request(&mut self, state: &DmmQuoteState, side: OrderSide) -> OrderRequest {
        let (price, quantity) = match side {
            OrderSide::Buy => (state.bid, state.bid_qty),
            OrderSide::Sell => (state.ask, state.ask_qty),
        };

        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: self.next_client_order_id("dmm"),
            symbol: state.symbol.clone(),
            token_side: TokenSide::Yes,
            side,
            order_type: OrderType::Limit,
            sizing: match side {
                OrderSide::Buy => PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd: UsdNotional::from_poly(quantity, price),
                    max_avg_price: price,
                    max_shares: quantity,
                },
                OrderSide::Sell => PolySizingInstruction::SellExactShares {
                    shares: quantity,
                    min_avg_price: price,
                },
            },
            time_in_force: TimeInForce::Gtc,
            post_only: true,
        })
    }

    fn requests_from_signal(&mut self, signal: &ArbSignalEvent) -> Vec<OrderRequest> {
        match &signal.action {
            ArbSignalAction::BasisLong {
                token_side,
                poly_side,
                poly_target_shares,
                poly_target_notional,
                cex_side,
                cex_hedge_qty,
                ..
            }
            | ArbSignalAction::BasisShort {
                token_side,
                poly_side,
                poly_target_shares,
                poly_target_notional,
                cex_side,
                cex_hedge_qty,
                ..
            } => {
                let effective_cex_qty = self.normalize_cex_qty(&signal.symbol, *cex_hedge_qty);
                vec![
                    self.poly_market_order(
                        signal.symbol.clone(),
                        *token_side,
                        *poly_side,
                        *poly_target_shares,
                        Some(*poly_target_notional),
                    ),
                    self.cex_market_order(signal.symbol.clone(), *cex_side, effective_cex_qty),
                ]
            }
            ArbSignalAction::DeltaRebalance {
                cex_side,
                cex_qty_adjust,
                ..
            } => {
                let effective_cex_qty = self.normalize_cex_qty(&signal.symbol, *cex_qty_adjust);
                if effective_cex_qty.0 <= Decimal::ZERO {
                    Vec::new()
                } else {
                    vec![self.cex_market_order(signal.symbol.clone(), *cex_side, effective_cex_qty)]
                }
            }
            ArbSignalAction::NegRiskArb { legs } => legs
                .iter()
                .map(|leg| {
                    self.poly_market_order(
                        leg.symbol.clone(),
                        leg.token_side,
                        leg.side,
                        leg.quantity,
                        None,
                    )
                })
                .collect(),
            ArbSignalAction::ClosePosition { .. } => self.close_orders_for_symbol(&signal.symbol),
        }
    }

    fn poly_market_order(
        &mut self,
        symbol: Symbol,
        token_side: TokenSide,
        side: OrderSide,
        shares: PolyShares,
        _quote_notional: Option<UsdNotional>,
    ) -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: self.next_client_order_id("arb-poly"),
            symbol,
            token_side,
            side,
            order_type: OrderType::Market,
            sizing: match side {
                OrderSide::Buy => PolySizingInstruction::BuyBudgetCap {
                    // Legacy signal path is still fixed-shares market execution until the
                    // planner hard-cut lands, so do not let the stale notional field shrink it.
                    max_cost_usd: UsdNotional::from_poly(shares, Price::ONE),
                    max_avg_price: Price::ONE,
                    max_shares: shares,
                },
                OrderSide::Sell => PolySizingInstruction::SellExactShares {
                    shares,
                    min_avg_price: Price::ZERO,
                },
            },
            time_in_force: TimeInForce::Fok,
            post_only: false,
        })
    }

    fn cex_market_order(
        &mut self,
        symbol: Symbol,
        side: OrderSide,
        base_qty: polyalpha_core::CexBaseQty,
    ) -> OrderRequest {
        self.cex_market_order_with_options(symbol, side, base_qty, false)
    }

    fn cex_market_order_with_options(
        &mut self,
        symbol: Symbol,
        side: OrderSide,
        base_qty: polyalpha_core::CexBaseQty,
        reduce_only: bool,
    ) -> OrderRequest {
        let (exchange, venue_symbol) = self.resolve_cex_target(&symbol);
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: self.next_client_order_id("arb-cex"),
            exchange,
            symbol,
            venue_symbol,
            side,
            order_type: OrderType::Market,
            price: None,
            base_qty,
            time_in_force: TimeInForce::Ioc,
            reduce_only,
        })
    }

    fn resolve_cex_target(&self, symbol: &Symbol) -> (Exchange, String) {
        if let Some(config) = self
            .symbol_registry
            .as_ref()
            .and_then(|registry| registry.get_config(symbol))
        {
            return (
                config.hedge_exchange,
                cex_venue_symbol(config.hedge_exchange, &config.cex_symbol),
            );
        }

        (
            Exchange::Binance,
            symbol.0.to_ascii_uppercase().replace('-', ""),
        )
    }

    fn normalize_cex_qty(
        &self,
        symbol: &Symbol,
        requested_qty: polyalpha_core::CexBaseQty,
    ) -> polyalpha_core::CexBaseQty {
        self.symbol_registry
            .as_ref()
            .and_then(|registry| registry.get_config(symbol))
            .map(|config| requested_qty.floor_to_step(config.cex_qty_step))
            .unwrap_or(requested_qty)
    }

    fn invalid_open_signal_reason(&self, signal: &ArbSignalEvent) -> Option<&'static str> {
        match &signal.action {
            ArbSignalAction::BasisLong { cex_hedge_qty, .. }
            | ArbSignalAction::BasisShort { cex_hedge_qty, .. } => {
                let effective_qty = self.normalize_cex_qty(&signal.symbol, *cex_hedge_qty);
                (effective_qty.0 <= Decimal::ZERO).then_some("zero_cex_hedge_qty")
            }
            _ => None,
        }
    }

    fn rejected_signal_event(&self, signal: &ArbSignalEvent, reason: &str) -> ExecutionEvent {
        let exchange = match &signal.action {
            ArbSignalAction::BasisLong { .. }
            | ArbSignalAction::BasisShort { .. }
            | ArbSignalAction::DeltaRebalance { .. } => self.resolve_cex_target(&signal.symbol).0,
            ArbSignalAction::NegRiskArb { .. } | ArbSignalAction::ClosePosition { .. } => {
                Exchange::Polymarket
            }
        };

        ExecutionEvent::OrderSubmitted {
            symbol: signal.symbol.clone(),
            exchange,
            response: OrderResponse {
                client_order_id: ClientOrderId(format!("rejected-{}", signal.signal_id)),
                exchange_order_id: OrderId(format!("rejected-{}", signal.signal_id)),
                status: OrderStatus::Rejected,
                filled_quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty::ZERO),
                average_price: None,
                rejection_reason: Some(reason.to_owned()),
                timestamp_ms: signal.timestamp_ms,
            },
            correlation_id: signal.correlation_id.clone(),
        }
    }

    fn next_client_order_id(&mut self, prefix: &str) -> ClientOrderId {
        let seq = self.next_client_order_seq;
        self.next_client_order_seq += 1;
        ClientOrderId(format!("{prefix}-{seq}"))
    }

    fn next_fill_id(&mut self) -> String {
        let seq = self.next_fill_seq;
        self.next_fill_seq += 1;
        format!("dry-fill-{seq}")
    }

    fn close_orders_for_symbol(&mut self, symbol: &Symbol) -> Vec<OrderRequest> {
        if self.position_tracker.symbol_is_flat(symbol) {
            return Vec::new();
        }

        let mut requests = Vec::new();
        let poly_yes_shares = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::PolyYes);
        if !poly_yes_shares.is_zero() {
            requests.push(self.poly_market_order(
                symbol.clone(),
                TokenSide::Yes,
                close_side(poly_yes_shares),
                PolyShares(poly_yes_shares.abs()),
                None,
            ));
        }
        let poly_no_shares = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::PolyNo);
        if !poly_no_shares.is_zero() {
            requests.push(self.poly_market_order(
                symbol.clone(),
                TokenSide::No,
                close_side(poly_no_shares),
                PolyShares(poly_no_shares.abs()),
                None,
            ));
        }
        let cex_base_qty = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::CexPerp);
        if !cex_base_qty.is_zero() {
            requests.push(self.cex_market_order_with_options(
                symbol.clone(),
                close_side(cex_base_qty),
                polyalpha_core::CexBaseQty(cex_base_qty.abs()),
                true,
            ));
        }

        requests
    }

    fn apply_fill_to_position(&mut self, fill: &Fill) {
        let effect = self.position_tracker.apply_fill(fill);

        if !effect.realized_pnl_delta.0.is_zero() {
            if let Some(session) = self.sessions.get_mut(&fill.symbol) {
                session.realized_pnl.0 += effect.realized_pnl_delta.0;
            }
        }
    }

    fn fill_from_request_and_response(
        &mut self,
        request: OrderRequest,
        response: OrderResponse,
        correlation_id: &str,
    ) -> Fill {
        match request {
            OrderRequest::Poly(req) => {
                let price = response.average_price.unwrap_or(Price::ONE);
                let quantity = response.filled_quantity;
                let notional_usd = match quantity {
                    VenueQuantity::PolyShares(shares) => shares.to_usd_notional(price),
                    VenueQuantity::CexBaseQty(qty) => UsdNotional(qty.0 * price.0),
                };
                Fill {
                    fill_id: self.next_fill_id(),
                    correlation_id: correlation_id.to_owned(),
                    exchange: Exchange::Polymarket,
                    symbol: req.symbol,
                    instrument: match req.token_side {
                        TokenSide::Yes => InstrumentKind::PolyYes,
                        TokenSide::No => InstrumentKind::PolyNo,
                    },
                    order_id: response.exchange_order_id,
                    side: req.side,
                    price,
                    quantity,
                    notional_usd,
                    fee: UsdNotional::ZERO,
                    is_maker: req.post_only
                        || matches!(req.order_type, OrderType::Limit | OrderType::PostOnly),
                    timestamp_ms: response.timestamp_ms,
                }
            }
            OrderRequest::Cex(req) => {
                let price = response.average_price.unwrap_or(Price::ONE);
                let quantity = response.filled_quantity;
                let notional_usd = match quantity {
                    VenueQuantity::PolyShares(shares) => shares.to_usd_notional(price),
                    VenueQuantity::CexBaseQty(qty) => UsdNotional(qty.0 * price.0),
                };
                Fill {
                    fill_id: self.next_fill_id(),
                    correlation_id: correlation_id.to_owned(),
                    exchange: req.exchange,
                    symbol: req.symbol,
                    instrument: InstrumentKind::CexPerp,
                    order_id: response.exchange_order_id,
                    side: req.side,
                    price,
                    quantity,
                    notional_usd,
                    fee: UsdNotional::ZERO,
                    is_maker: matches!(req.order_type, OrderType::Limit | OrderType::PostOnly),
                    timestamp_ms: response.timestamp_ms,
                }
            }
        }
    }
}

fn close_side(net_qty: Decimal) -> OrderSide {
    if net_qty > Decimal::ZERO {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}

fn empty_orderbook(
    exchange: Exchange,
    symbol: Symbol,
    instrument: InstrumentKind,
) -> polyalpha_core::OrderBookSnapshot {
    polyalpha_core::OrderBookSnapshot {
        exchange,
        symbol,
        instrument,
        bids: Vec::new(),
        asks: Vec::new(),
        exchange_timestamp_ms: 0,
        received_at_ms: 0,
        sequence: 0,
        last_trade_price: None,
    }
}

fn has_filled_quantity(quantity: &VenueQuantity) -> bool {
    match quantity {
        VenueQuantity::PolyShares(shares) => shares.0 > Decimal::ZERO,
        VenueQuantity::CexBaseQty(qty) => qty.0 > Decimal::ZERO,
    }
}

fn residual_cex_delta(plan: &TradePlan, fill: Option<&Fill>) -> f64 {
    let filled_qty = fill
        .map(|item| match item.quantity {
            VenueQuantity::CexBaseQty(qty) => qty.0,
            VenueQuantity::PolyShares(_) => Decimal::ZERO,
        })
        .unwrap_or(Decimal::ZERO);
    (plan.cex_planned_qty.0 - filled_qty)
        .abs()
        .to_f64()
        .unwrap_or_default()
}

fn response_notional(response: &OrderResponse, price: Price) -> UsdNotional {
    match response.filled_quantity {
        VenueQuantity::PolyShares(shares) => shares.to_usd_notional(price),
        VenueQuantity::CexBaseQty(qty) => UsdNotional(qty.0 * price.0),
    }
}

fn order_ledger_entry(request: &OrderRequest, response: &OrderResponse) -> OrderLedgerEntry {
    let requested_qty = match request {
        OrderRequest::Poly(req) => req.sizing.requested_shares().0.to_string(),
        OrderRequest::Cex(req) => req.base_qty.0.to_string(),
    };
    let filled_qty = match response.filled_quantity {
        VenueQuantity::PolyShares(shares) => shares.0.to_string(),
        VenueQuantity::CexBaseQty(qty) => qty.0.to_string(),
    };

    OrderLedgerEntry {
        order_id: response.exchange_order_id.0.clone(),
        client_order_id: response.client_order_id.0.clone(),
        requested_qty,
        filled_qty: filled_qty.clone(),
        cancelled_qty: Decimal::ZERO.to_string(),
        avg_price: response.average_price.unwrap_or(Price::ZERO).0.to_string(),
        fee_usd: 0.0,
        exchange_timestamp_ms: response.timestamp_ms,
        received_at_ms: response.timestamp_ms,
        status: format!("{:?}", response.status).to_ascii_lowercase(),
    }
}

fn best_bid_or_ask(book: &polyalpha_core::OrderBookSnapshot, side: OrderSide) -> Price {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal::Decimal;

    use polyalpha_core::{
        ArbSignalAction, MarketConfig, OpenCandidate, PlanningIntent, PolymarketIds,
        SignalStrength, PLANNING_SCHEMA_VERSION,
    };

    use crate::{
        dry_run::{DryRunExecutor, SlippageConfig},
        orderbook_provider::InMemoryOrderbookProvider,
    };

    use super::*;

    fn sample_symbol() -> Symbol {
        Symbol::new("btc-100k-mar-2026")
    }

    fn sample_market(exchange: Exchange, cex_symbol: &str) -> MarketConfig {
        MarketConfig {
            symbol: sample_symbol(),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some(
                "Will the price of Bitcoin be above $100,000 on March 31, 2026?".to_owned(),
            ),
            market_rule: Some(polyalpha_core::MarketRule::fallback_above(Price(
                Decimal::new(100_000, 0),
            ))),
            cex_symbol: cex_symbol.to_owned(),
            hedge_exchange: exchange,
            strike_price: Some(Price(Decimal::new(100_000, 0))),
            settlement_timestamp: 1_775_001_600,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn sample_basis_signal() -> ArbSignalEvent {
        ArbSignalEvent {
            signal_id: "sig-basis-1".to_owned(),
            correlation_id: "corr-basis-1".to_owned(),
            symbol: sample_symbol(),
            action: ArbSignalAction::BasisLong {
                token_side: TokenSide::Yes,
                poly_side: OrderSide::Buy,
                poly_target_shares: PolyShares(Decimal::new(25, 0)),
                poly_target_notional: UsdNotional(Decimal::new(12, 1)),
                cex_side: OrderSide::Sell,
                cex_hedge_qty: polyalpha_core::CexBaseQty(Decimal::new(3, 1)),
                delta: 0.012,
            },
            strength: SignalStrength::Strong,
            basis_value: None,
            z_score: None,
            expected_pnl: UsdNotional(Decimal::new(3, 0)),
            timestamp_ms: 1_715_000_000_123,
        }
    }

    fn sample_close_signal() -> ArbSignalEvent {
        ArbSignalEvent {
            signal_id: "sig-close-1".to_owned(),
            correlation_id: "corr-close-1".to_owned(),
            symbol: sample_symbol(),
            action: ArbSignalAction::ClosePosition {
                reason: "basis reverted".to_owned(),
            },
            strength: SignalStrength::Normal,
            basis_value: None,
            z_score: None,
            expected_pnl: UsdNotional::ZERO,
            timestamp_ms: 1_715_000_000_456,
        }
    }

    fn sample_open_intent() -> PlanningIntent {
        PlanningIntent::OpenPosition {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-open-1".to_owned(),
            correlation_id: "corr-open-1".to_owned(),
            symbol: sample_symbol(),
            candidate: OpenCandidate {
                schema_version: PLANNING_SCHEMA_VERSION,
                candidate_id: "cand-open-1".to_owned(),
                correlation_id: "corr-open-1".to_owned(),
                symbol: sample_symbol(),
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
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-force-1".to_owned(),
            correlation_id: "corr-open-1".to_owned(),
            symbol: sample_symbol(),
            force_reason: "operator_force_exit".to_owned(),
            allow_negative_edge: true,
        }
    }

    fn seed_liquidity(
        provider: &InMemoryOrderbookProvider,
        poly_liquidity: Decimal,
        cex_liquidity: Decimal,
    ) {
        provider.update(polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: sample_symbol(),
            instrument: InstrumentKind::PolyYes,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(49, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(51, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });
        provider.update(polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: sample_symbol(),
            instrument: InstrumentKind::PolyNo,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(49, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(51, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(poly_liquidity)),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });

        let cex_snapshot = polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: sample_symbol(),
            instrument: InstrumentKind::CexPerp,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(100_000, 0)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(cex_liquidity)),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(100_010, 0)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(cex_liquidity)),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        };
        provider.update_cex(cex_snapshot.clone(), "BTC100KMAR2026".to_owned());
        provider.update_cex(cex_snapshot, "BTCUSDT".to_owned());
    }

    fn sample_executor_with_liquidity(
        poly_liquidity: Decimal,
        cex_liquidity: Decimal,
        allow_partial_fill: bool,
    ) -> DryRunExecutor {
        let provider = Arc::new(InMemoryOrderbookProvider::new());
        seed_liquidity(&provider, poly_liquidity, cex_liquidity);
        DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                allow_partial_fill,
                min_liquidity: Decimal::ONE,
                ..SlippageConfig::default()
            },
        )
    }

    fn sample_planning_provider(
        poly_liquidity: Decimal,
        cex_liquidity: Decimal,
    ) -> Arc<InMemoryOrderbookProvider> {
        let provider = Arc::new(InMemoryOrderbookProvider::new());
        seed_liquidity(&provider, poly_liquidity, cex_liquidity);
        provider
    }

    fn fill_capable_executor() -> DryRunExecutor {
        let provider = Arc::new(InMemoryOrderbookProvider::new());

        provider.update(polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: sample_symbol(),
            instrument: InstrumentKind::PolyYes,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(49, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(51, 2)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });

        let cex_snapshot = polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: sample_symbol(),
            instrument: InstrumentKind::CexPerp,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(1000, 1)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::ONE)),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(1010, 1)),
                quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::ONE)),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        };
        provider.update_cex(cex_snapshot.clone(), "BTC100KMAR2026".to_owned());
        provider.update_cex(cex_snapshot, "BTCUSDT".to_owned());

        DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        )
    }

    #[tokio::test]
    async fn higher_priority_intent_supersedes_open_plan() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        manager.process_intent(sample_open_intent()).await.unwrap();
        let events = manager
            .process_intent(sample_force_exit_intent())
            .await
            .unwrap();

        assert!(events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::PlanSuperseded { .. })));
    }

    #[tokio::test]
    async fn poly_fill_spawns_child_hedge_plan() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(5, 0), Decimal::new(5, 0), true);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();

        assert!(events.iter().any(|event| matches!(
            event,
            ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_some()
        )));
    }

    #[tokio::test]
    async fn recovery_plan_is_emitted_when_cex_hedge_fails() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::ZERO, false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();

        assert!(events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::RecoveryPlanCreated { .. })));
    }

    #[tokio::test]
    async fn dmm_quote_update_submit_then_clear_emits_expected_events() {
        let executor = DryRunExecutor::new();
        let mut manager = ExecutionManager::new(executor);
        let symbol = sample_symbol();

        let set_events = manager
            .apply_dmm_quote_update(DmmQuoteUpdate::set(DmmQuoteState {
                symbol: symbol.clone(),
                bid: Price(Decimal::new(49, 2)),
                ask: Price(Decimal::new(51, 2)),
                bid_qty: PolyShares(Decimal::new(10, 0)),
                ask_qty: PolyShares(Decimal::new(10, 0)),
                updated_at_ms: 1_715_000_000_000,
            }))
            .await
            .expect("set quote should succeed");

        assert_eq!(
            set_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderSubmitted { .. }))
                .count(),
            2
        );
        assert!(manager.dmm_quote_state(&symbol).is_some());

        let clear_events = manager
            .apply_dmm_quote_update(DmmQuoteUpdate::clear(symbol.clone()))
            .await
            .expect("clear quote should succeed");

        assert_eq!(
            clear_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderCancelled { .. }))
                .count(),
            2
        );
        assert!(manager.dmm_quote_state(&symbol).is_none());
    }

    #[tokio::test]
    async fn basis_signal_generates_submissions_fills_and_state_transitions() {
        let executor = fill_capable_executor();
        let mut manager = ExecutionManager::new(executor);
        let signal = sample_basis_signal();

        let events = manager
            .process_arb_signal(signal)
            .await
            .expect("arb signal should process");

        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderSubmitted { .. }))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderFilled(_)))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::HedgeStateChanged { .. }))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn registry_backed_cex_orders_use_configured_symbol_and_exchange() {
        let executor = DryRunExecutor::new();
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Binance, "BTCUSDT")]);
        let mut manager = ExecutionManager::with_symbol_registry(executor.clone(), registry);

        manager
            .process_arb_signal(sample_basis_signal())
            .await
            .expect("arb signal should process");

        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        let hedge_order = orders
            .iter()
            .find(|order| order.exchange == Exchange::Binance)
            .expect("binance hedge order should exist");

        assert_eq!(hedge_order.symbol.0, sample_symbol().0);
        assert_eq!(hedge_order.venue_symbol.as_deref(), Some("BTCUSDT"));
    }

    #[tokio::test]
    async fn registry_backed_okx_orders_use_swap_inst_id() {
        let executor = DryRunExecutor::new();
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Okx, "BTCUSDT")]);
        let mut manager = ExecutionManager::with_symbol_registry(executor.clone(), registry);

        manager
            .process_arb_signal(sample_basis_signal())
            .await
            .expect("arb signal should process");

        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        let hedge_order = orders
            .iter()
            .find(|order| order.exchange == Exchange::Okx)
            .expect("okx hedge order should exist");

        assert_eq!(hedge_order.symbol.0, sample_symbol().0);
        assert_eq!(hedge_order.venue_symbol.as_deref(), Some("BTC-USDT-SWAP"));
    }

    #[tokio::test]
    async fn close_position_signal_submits_flattening_orders_and_returns_idle() {
        let executor = fill_capable_executor();
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Binance, "BTCUSDT")]);
        let mut manager = ExecutionManager::with_symbol_registry(executor.clone(), registry);

        manager
            .process_arb_signal(sample_basis_signal())
            .await
            .expect("basis signal should process");

        let close_events = manager
            .process_arb_signal(sample_close_signal())
            .await
            .expect("close signal should process");

        assert_eq!(
            close_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderSubmitted { .. }))
                .count(),
            2
        );
        assert_eq!(
            close_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderFilled(_)))
                .count(),
            2
        );
        assert_eq!(
            close_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::TradeClosed { .. }))
                .count(),
            1
        );
        let trade_closed = close_events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradeClosed { realized_pnl, .. } => Some(realized_pnl),
                _ => None,
            })
            .expect("trade closed event should exist");
        assert_eq!(*trade_closed, UsdNotional(Decimal::new(-93_706, 5)));
        assert_eq!(
            manager.hedge_state(&sample_symbol()),
            Some(HedgeState::Idle)
        );

        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        assert_eq!(orders.len(), 4);
    }

    #[tokio::test]
    async fn basis_signal_with_zero_effective_cex_hedge_qty_is_rejected_before_submission() {
        let executor = fill_capable_executor();
        let mut market = sample_market(Exchange::Binance, "BTCUSDT");
        market.cex_qty_step = Decimal::ONE;
        let registry = SymbolRegistry::new(vec![market]);
        let mut manager = ExecutionManager::with_symbol_registry(executor.clone(), registry);
        let mut signal = sample_basis_signal();
        if let ArbSignalAction::BasisLong { cex_hedge_qty, .. } = &mut signal.action {
            *cex_hedge_qty = polyalpha_core::CexBaseQty(Decimal::new(3, 1));
        }

        let events = manager
            .process_arb_signal(signal)
            .await
            .expect("invalid open signal should return a rejection event");

        assert_eq!(events.len(), 1);
        match &events[0] {
            ExecutionEvent::OrderSubmitted { response, .. } => {
                assert_eq!(response.status, OrderStatus::Rejected);
                assert_eq!(
                    response.rejection_reason.as_deref(),
                    Some("zero_cex_hedge_qty")
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(manager.hedge_state(&sample_symbol()), None);

        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        assert!(
            orders.is_empty(),
            "invalid open signal must not submit any exchange order"
        );
    }
}
