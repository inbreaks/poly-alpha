use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use uuid::Uuid;

use polyalpha_core::{
    cex_venue_symbol, CexOrderRequest, ClientOrderId, CoreError, DmmQuoteState, DmmQuoteUpdate,
    Exchange, ExecutionEvent, ExecutionResult, Fill, HedgeState, InstrumentKind, OrderExecutor,
    OrderId, OrderLedgerEntry, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType,
    PlanRejectionReason, PlanningIntent, PolyOrderRequest, PolySizingInstruction, PositionTracker,
    Price, Result, Symbol, SymbolRegistry, TimeInForce, TokenSide, TradePlan, UsdNotional,
    VenueQuantity, PLANNING_SCHEMA_VERSION,
};

use crate::{
    orderbook_provider::{OrderbookKey, OrderbookProvider},
    plan_state::{priority_rank, InFlightPlan, PlanLifecycleState},
    planner::{CanonicalPlanningContext, ExecutionPlanner, PlanRejection},
};

#[derive(Debug)]
pub enum PreviewIntentError {
    Core(CoreError),
    PlanRejected(PlanRejection),
}

impl std::fmt::Display for PreviewIntentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(err) => write!(f, "{err}"),
            Self::PlanRejected(rejection) => write!(
                f,
                "plan rejected [{}]: {}",
                rejection.reason.code(),
                rejection.detail
            ),
        }
    }
}

impl std::error::Error for PreviewIntentError {}

fn preview_intent_error_to_core(err: PreviewIntentError) -> CoreError {
    match err {
        PreviewIntentError::Core(err) => err,
        PreviewIntentError::PlanRejected(rejection) => CoreError::Generic(format!(
            "plan rejected [{}]: {}",
            rejection.reason.code(),
            rejection.detail
        )),
    }
}

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

#[derive(Clone, Debug)]
struct ObservedBookState {
    sequence: u64,
    fingerprint: String,
}

#[derive(Clone, Debug)]
struct QueuedPlanExecution {
    plan: TradePlan,
    remaining_auto_recovery_hops: usize,
    next_leg: PlannedExecutionLeg,
}

#[derive(Clone, Debug)]
enum FollowUpPlan {
    HedgeChild(TradePlan),
    Recovery(TradePlan),
}

#[derive(Clone, Copy, Debug)]
enum PlannedExecutionLeg {
    Inferred,
    CexOnly,
}

#[derive(Default)]
struct PlanExecutionOutcome {
    events: Vec<ExecutionEvent>,
    follow_up_plan: Option<FollowUpPlan>,
}

const AUTO_RECOVERY_PLAN_HOPS: usize = 1;

#[derive(Clone)]
pub struct ExecutionManager<E: OrderExecutor> {
    executor: E,
    symbol_registry: Option<SymbolRegistry>,
    orderbook_provider: Option<Arc<dyn OrderbookProvider>>,
    planner: ExecutionPlanner,
    planner_overrides: HashMap<Symbol, ExecutionPlanner>,
    planner_depth_levels: usize,
    dmm_quote_states: HashMap<Symbol, DmmQuoteState>,
    active_dmm_orders: HashMap<Symbol, ActiveDmmOrders>,
    active_plans: HashMap<Symbol, InFlightPlan>,
    observed_books: HashMap<OrderbookKey, ObservedBookState>,
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
            planner_overrides: HashMap::new(),
            planner_depth_levels: 1,
            dmm_quote_states: HashMap::new(),
            active_dmm_orders: HashMap::new(),
            active_plans: HashMap::new(),
            observed_books: HashMap::new(),
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

    pub fn with_planner(mut self, planner: ExecutionPlanner) -> Self {
        self.planner = planner;
        self
    }

    pub fn with_planner_overrides(
        mut self,
        planner_overrides: HashMap<Symbol, ExecutionPlanner>,
    ) -> Self {
        self.planner_overrides = planner_overrides;
        self
    }

    pub fn with_planner_depth_levels(mut self, levels: usize) -> Self {
        self.planner_depth_levels = levels.max(1);
        self
    }

    fn planner_for_symbol(&self, symbol: &Symbol) -> ExecutionPlanner {
        self.planner_overrides
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| self.planner.clone())
    }

    pub fn planning_context_for_symbol(
        &mut self,
        symbol: &Symbol,
    ) -> Result<CanonicalPlanningContext> {
        self.build_planning_context_for_symbol(symbol)
    }

    pub fn preview_intent(&mut self, intent: &PlanningIntent) -> Result<TradePlan> {
        self.preview_intent_detailed(intent)
            .map_err(preview_intent_error_to_core)
    }

    pub fn preview_intent_detailed(
        &mut self,
        intent: &PlanningIntent,
    ) -> std::result::Result<TradePlan, PreviewIntentError> {
        self.plan_intent_detailed(intent)
    }

    pub fn hedge_state(&self, symbol: &Symbol) -> Option<HedgeState> {
        self.sessions.get(symbol).map(|session| session.state)
    }

    pub fn active_plan_priority(&self, symbol: &Symbol) -> Option<&str> {
        self.active_plans
            .get(symbol)
            .map(|in_flight| in_flight.plan.priority.as_str())
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

    pub async fn process_intent(&mut self, intent: PlanningIntent) -> Result<Vec<ExecutionEvent>> {
        let plan = self.plan_intent(&intent)?;
        self.activate_and_execute_plan(plan).await
    }

    pub async fn process_plan(&mut self, plan: TradePlan) -> Result<Vec<ExecutionEvent>> {
        self.validate_external_plan(&plan)?;
        self.activate_and_execute_plan(plan).await
    }

    fn validate_external_plan(&mut self, plan: &TradePlan) -> Result<()> {
        if plan.intent_type == "open_position" && plan.cex_planned_qty.0 <= Decimal::ZERO {
            return Err(CoreError::Generic(format!(
                "plan rejected [{}]: external open plan requires non-zero cex hedge qty",
                PlanRejectionReason::ZeroCexHedgeQty.code(),
            )));
        }
        let latest_context = self.build_planning_context_for_symbol(&plan.symbol)?;
        let planner = self.planner_for_symbol(&plan.symbol);
        if let Err(reason) = planner.revalidate(plan, &latest_context) {
            return Err(CoreError::Generic(format!(
                "plan rejected [{}]: external plan failed revalidation",
                reason.code()
            )));
        }
        Ok(())
    }

    async fn activate_and_execute_plan(
        &mut self,
        mut plan: TradePlan,
    ) -> Result<Vec<ExecutionEvent>> {
        let mut events = Vec::new();
        if let Some(existing) = self.active_plans.get(&plan.symbol).cloned() {
            let incoming_rank = priority_rank(&plan.priority);
            if existing.idempotency_key == plan.idempotency_key {
                return Ok(events);
            }
            if incoming_rank > existing.priority_rank() {
                plan.supersedes_plan_id = Some(existing.plan.plan_id.clone());
                events.push(ExecutionEvent::PlanSuperseded {
                    symbol: plan.symbol.clone(),
                    superseded_plan_id: existing.plan.plan_id.clone(),
                    next_plan_id: plan.plan_id.clone(),
                });
            } else {
                return Err(CoreError::Generic(format!(
                    "plan rejected [{}]: higher priority plan active for {}",
                    polyalpha_core::PlanRejectionReason::HigherPriorityPlanActive.code(),
                    plan.symbol.0
                )));
            }
        }

        self.session_identity(&plan.symbol);
        if let Some(session) = self.sessions.get_mut(&plan.symbol) {
            session.correlation_id = Some(plan.correlation_id.clone());
            session.last_signal_id = Some(plan.plan_id.clone());
        }
        self.install_plan(plan.clone(), PlanLifecycleState::PlanReady);
        events.push(ExecutionEvent::TradePlanCreated { plan: plan.clone() });

        if self.plan_is_noop(&plan) {
            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
            return Ok(events);
        }

        events.extend(
            self.process_plan_queue(QueuedPlanExecution {
                plan,
                remaining_auto_recovery_hops: AUTO_RECOVERY_PLAN_HOPS,
                next_leg: PlannedExecutionLeg::Inferred,
            })
            .await?,
        );
        Ok(events)
    }

    async fn process_plan_queue(
        &mut self,
        initial: QueuedPlanExecution,
    ) -> Result<Vec<ExecutionEvent>> {
        let mut pending = VecDeque::from([initial]);
        let mut events = Vec::new();

        while let Some(queued) = pending.pop_front() {
            if let Some(active) = self.active_plans.get(&queued.plan.symbol) {
                if active.plan.plan_id != queued.plan.plan_id {
                    return Err(CoreError::Generic(format!(
                        "plan rejected [{}]: {}",
                        polyalpha_core::RevalidationFailureReason::PlanSuperseded.code(),
                        queued.plan.plan_id
                    )));
                }
            }

            let outcome = match queued.next_leg {
                PlannedExecutionLeg::CexOnly => {
                    if queued.plan.cex_planned_qty.0 > Decimal::ZERO {
                        self.install_plan(queued.plan.clone(), PlanLifecycleState::HedgingCex);
                        self.submit_cex_leg(&queued.plan).await?
                    } else {
                        self.update_plan_state(&queued.plan.symbol, PlanLifecycleState::Frozen);
                        PlanExecutionOutcome::default()
                    }
                }
                PlannedExecutionLeg::Inferred => {
                    if queued.plan.poly_planned_shares.0 > Decimal::ZERO {
                        self.install_plan(queued.plan.clone(), PlanLifecycleState::SubmittingPoly);
                        self.submit_poly_leg(&queued.plan).await?
                    } else if queued.plan.cex_planned_qty.0 > Decimal::ZERO {
                        self.install_plan(queued.plan.clone(), PlanLifecycleState::HedgingCex);
                        self.submit_cex_leg(&queued.plan).await?
                    } else {
                        self.update_plan_state(&queued.plan.symbol, PlanLifecycleState::Frozen);
                        PlanExecutionOutcome::default()
                    }
                }
            };

            events.extend(outcome.events);
            if let Some(follow_up_plan) = outcome.follow_up_plan {
                match follow_up_plan {
                    FollowUpPlan::HedgeChild(plan) => {
                        if self.plan_is_noop(&plan) {
                            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
                        } else {
                            pending.push_front(QueuedPlanExecution {
                                plan,
                                remaining_auto_recovery_hops: queued.remaining_auto_recovery_hops,
                                next_leg: PlannedExecutionLeg::CexOnly,
                            });
                        }
                    }
                    FollowUpPlan::Recovery(plan) => {
                        if self.plan_is_noop(&plan) {
                            self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
                        } else if queued.remaining_auto_recovery_hops > 0 {
                            pending.push_front(QueuedPlanExecution {
                                plan,
                                remaining_auto_recovery_hops: queued
                                    .remaining_auto_recovery_hops
                                    .saturating_sub(1),
                                next_leg: PlannedExecutionLeg::Inferred,
                            });
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    fn plan_intent_detailed(
        &mut self,
        intent: &PlanningIntent,
    ) -> std::result::Result<TradePlan, PreviewIntentError> {
        let planner = self.planner_for_symbol(intent.symbol());
        let initial_context = self
            .build_planning_context(intent)
            .map_err(PreviewIntentError::Core)?;
        let mut plan = planner
            .plan(intent, &initial_context)
            .map_err(PreviewIntentError::PlanRejected)?;
        let latest_context = self
            .build_planning_context(intent)
            .map_err(PreviewIntentError::Core)?;
        if let Err(reason) = planner.revalidate(&plan, &latest_context) {
            plan = planner
                .plan(intent, &latest_context)
                .map_err(|mut rejection| {
                    rejection.detail = format!(
                        "plan revalidation failed [{}] and replan rejected [{}]: {}",
                        reason.code(),
                        rejection.reason.code(),
                        rejection.detail
                    );
                    PreviewIntentError::PlanRejected(rejection)
                })?;
        }
        Ok(plan)
    }

    fn plan_intent(&mut self, intent: &PlanningIntent) -> Result<TradePlan> {
        self.plan_intent_detailed(intent)
            .map_err(preview_intent_error_to_core)
    }

    fn build_planning_context(
        &mut self,
        intent: &PlanningIntent,
    ) -> Result<CanonicalPlanningContext> {
        self.build_planning_context_for_symbol(intent.symbol())
    }

    fn build_planning_context_for_symbol(
        &mut self,
        symbol: &Symbol,
    ) -> Result<CanonicalPlanningContext> {
        let provider = self.orderbook_provider.clone().ok_or_else(|| {
            CoreError::Generic("planning orderbook provider is unavailable".to_owned())
        })?;
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
        let current_poly_yes_shares = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::PolyYes);
        let current_poly_no_shares = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::PolyNo);
        let current_cex_net_qty = self
            .position_tracker
            .net_symbol_qty(symbol, InstrumentKind::CexPerp);
        let cex_qty_step = self
            .symbol_registry
            .as_ref()
            .and_then(|registry| registry.get_config(symbol))
            .map(|config| config.cex_qty_step)
            .unwrap_or(Decimal::ZERO);
        let context = CanonicalPlanningContext {
            planner_depth_levels: self.planner_depth_levels,
            poly_yes_book,
            poly_no_book,
            cex_book,
            cex_qty_step,
            current_poly_yes_shares,
            current_poly_no_shares,
            current_cex_net_qty,
            now_ms,
        }
        .canonicalize();

        self.observe_book_snapshot(&context.poly_yes_book)?;
        self.observe_book_snapshot(&context.poly_no_book)?;
        self.observe_book_snapshot(&context.cex_book)?;

        Ok(context)
    }

    fn observe_book_snapshot(&mut self, book: &polyalpha_core::OrderBookSnapshot) -> Result<()> {
        if book.bids.is_empty() && book.asks.is_empty() {
            return Ok(());
        }

        let key = OrderbookKey {
            exchange: book.exchange,
            symbol: book.symbol.clone(),
            instrument: book.instrument,
        };
        let fingerprint = serde_json::to_string(book).map_err(|err| {
            CoreError::Generic(format!("failed to fingerprint planning book: {err}"))
        })?;
        if let Some(previous) = self.observed_books.get(&key) {
            if book.sequence < previous.sequence {
                return Err(CoreError::Generic(format!(
                    "planning context rejected [{}]: instrument={:?} sequence={} previous_sequence={}",
                    PlanRejectionReason::StaleBookSequence.code(),
                    key.instrument,
                    book.sequence,
                    previous.sequence
                )));
            }
            if book.sequence == previous.sequence && fingerprint != previous.fingerprint {
                return Err(CoreError::Generic(format!(
                    "planning context rejected [{}]: instrument={:?} sequence={}",
                    PlanRejectionReason::BookConflictSameSequence.code(),
                    key.instrument,
                    book.sequence
                )));
            }
        }
        self.observed_books.insert(
            key,
            ObservedBookState {
                sequence: book.sequence,
                fingerprint,
            },
        );
        Ok(())
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
        if state == PlanLifecycleState::Frozen {
            self.active_plans.remove(symbol);
            return;
        }
        if let Some(active) = self.active_plans.get_mut(symbol) {
            active.state = state;
        }
    }

    async fn submit_poly_leg(&mut self, plan: &TradePlan) -> Result<PlanExecutionOutcome> {
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
                request.clone(),
                response.clone(),
                &plan.correlation_id,
            );
            self.apply_fill_to_position(&fill);
            events.push(ExecutionEvent::OrderFilled(fill.clone()));
            let outcome = self.on_poly_fill(plan, &request, &response, &fill).await?;
            events.extend(outcome.events);
            Ok(PlanExecutionOutcome {
                events,
                follow_up_plan: outcome.follow_up_plan,
            })
        } else {
            let outcome = self.complete_plan_after_execution(
                plan,
                Some(&request),
                Some(&response),
                UsdNotional::ZERO,
                0.0,
            )?;
            events.extend(outcome.events);
            Ok(PlanExecutionOutcome {
                events,
                follow_up_plan: outcome.follow_up_plan,
            })
        }
    }

    async fn on_poly_fill(
        &mut self,
        plan: &TradePlan,
        request: &OrderRequest,
        response: &OrderResponse,
        fill: &Fill,
    ) -> Result<PlanExecutionOutcome> {
        let mut events = Vec::new();
        let Some(child_plan) = self.child_hedge_plan_from_fill(plan, fill)? else {
            let outcome = self.complete_plan_after_execution(
                plan,
                Some(request),
                Some(response),
                fill.notional_usd,
                0.0,
            )?;
            events.extend(outcome.events);
            return Ok(PlanExecutionOutcome {
                events,
                follow_up_plan: outcome.follow_up_plan,
            });
        };

        self.install_plan(child_plan.clone(), PlanLifecycleState::PlanReady);
        events.push(ExecutionEvent::TradePlanCreated {
            plan: child_plan.clone(),
        });
        Ok(PlanExecutionOutcome {
            events,
            follow_up_plan: Some(FollowUpPlan::HedgeChild(child_plan)),
        })
    }

    async fn submit_cex_leg(&mut self, plan: &TradePlan) -> Result<PlanExecutionOutcome> {
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

        let residual_delta = residual_cex_delta(plan, cex_fill.as_ref());
        let outcome = self.complete_plan_after_execution(
            plan,
            Some(&request),
            Some(&response),
            plan.poly_max_cost_usd,
            residual_delta,
        )?;
        events.extend(outcome.events);
        Ok(PlanExecutionOutcome {
            events,
            follow_up_plan: outcome.follow_up_plan,
        })
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
        &mut self,
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

        let context = self.build_planning_context_for_symbol(&parent_plan.symbol)?;
        match self
            .planner
            .replan_from_execution(parent_plan, std::slice::from_ref(fill), &context)
        {
            Ok(mut child_plan) => {
                child_plan.parent_plan_id = Some(parent_plan.plan_id.clone());
                Ok(Some(child_plan))
            }
            Err(rejection)
                if matches!(
                    rejection.reason,
                    PlanRejectionReason::ResidualDeltaTooLarge
                        | PlanRejectionReason::InsufficientCexDepth
                        | PlanRejectionReason::MissingCexBook
                        | PlanRejectionReason::OneSidedCexBook
                ) =>
            {
                Ok(None)
            }
            Err(rejection) => Err(CoreError::Generic(format!(
                "child hedge replan rejected [{}]: {}",
                rejection.reason.code(),
                rejection.detail
            ))),
        }
    }

    fn execution_result_for_plan(
        &self,
        plan: &TradePlan,
        request: Option<&OrderRequest>,
        response: Option<&OrderResponse>,
        actual_poly_cost_usd: UsdNotional,
        actual_residual_delta: f64,
    ) -> ExecutionResult {
        let response_cost = response
            .and_then(|item| {
                item.average_price
                    .map(|price| response_notional(item, price))
            })
            .unwrap_or(UsdNotional::ZERO);
        let response_poly_cost = match request {
            Some(OrderRequest::Poly(_)) => response_cost,
            _ => UsdNotional::ZERO,
        };
        let response_cex_cost = match request {
            Some(OrderRequest::Cex(_)) => response_cost,
            _ => UsdNotional::ZERO,
        };
        let actual_poly_cost_usd = if response_poly_cost.0 > Decimal::ZERO {
            response_poly_cost
        } else {
            actual_poly_cost_usd
        };
        let actual_cex_cost_usd = response_cex_cost;
        let economics = economics_for_notionals(
            plan,
            &self.planner,
            actual_poly_cost_usd,
            actual_cex_cost_usd,
        );
        let shock_reference_price = response
            .and_then(|item| item.average_price)
            .filter(|_| matches!(request, Some(OrderRequest::Cex(_))))
            .unwrap_or(plan.cex_executable_price);
        let [actual_shock_loss_up_1pct, actual_shock_loss_down_1pct, actual_shock_loss_up_2pct, actual_shock_loss_down_2pct] =
            shock_losses_for_residual_delta(actual_residual_delta, shock_reference_price);
        let (poly_order_ledger, cex_order_ledger) = match (request, response) {
            (Some(request @ OrderRequest::Poly(_)), Some(response)) => (
                vec![order_ledger_entry(
                    request,
                    response,
                    economics.poly_fee_usd,
                )],
                Vec::new(),
            ),
            (Some(request @ OrderRequest::Cex(_)), Some(response)) => (
                Vec::new(),
                vec![order_ledger_entry(request, response, economics.cex_fee_usd)],
            ),
            _ => (Vec::new(), Vec::new()),
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
            poly_order_ledger,
            cex_order_ledger,
            actual_poly_cost_usd,
            actual_cex_cost_usd,
            actual_poly_fee_usd: economics.poly_fee_usd,
            actual_cex_fee_usd: economics.cex_fee_usd,
            actual_funding_cost_usd: economics.funding_cost_usd,
            realized_edge_usd: economics.realized_edge_usd,
            plan_vs_fill_deviation_usd: UsdNotional(
                plan.planned_edge_usd.0 - economics.realized_edge_usd.0,
            ),
            actual_residual_delta,
            actual_shock_loss_up_1pct,
            actual_shock_loss_down_1pct,
            actual_shock_loss_up_2pct,
            actual_shock_loss_down_2pct,
            recovery_required: actual_residual_delta > plan.max_residual_delta,
            timestamp_ms: response
                .map(|item| item.timestamp_ms)
                .unwrap_or(plan.created_at_ms),
        }
    }

    fn complete_plan_after_execution(
        &mut self,
        plan: &TradePlan,
        request: Option<&OrderRequest>,
        response: Option<&OrderResponse>,
        actual_poly_cost_usd: UsdNotional,
        actual_residual_delta: f64,
    ) -> Result<PlanExecutionOutcome> {
        self.update_plan_state(&plan.symbol, PlanLifecycleState::VerifyingResidual);
        let result = self.execution_result_for_plan(
            plan,
            request,
            response,
            actual_poly_cost_usd,
            actual_residual_delta,
        );
        self.apply_execution_result_to_session(&plan.symbol, &result);
        let mut events = vec![ExecutionEvent::ExecutionResultRecorded {
            result: result.clone(),
        }];
        let follow_up_plan =
            if let Some(recovery_plan) = self.recovery_plan_from_result(plan, &result)? {
                self.install_plan(recovery_plan.clone(), PlanLifecycleState::Recovering);
                events.push(ExecutionEvent::RecoveryPlanCreated {
                    plan: recovery_plan.clone(),
                });
                Some(FollowUpPlan::Recovery(recovery_plan))
            } else {
                self.update_plan_state(&plan.symbol, PlanLifecycleState::Frozen);
                None
            };
        if follow_up_plan.is_none() {
            if let Some(trade_closed) = self.maybe_trade_closed_event(&plan.symbol, &result) {
                events.push(trade_closed);
            }
        }

        Ok(PlanExecutionOutcome {
            events,
            follow_up_plan,
        })
    }

    fn recovery_plan_from_result(
        &mut self,
        plan: &TradePlan,
        result: &ExecutionResult,
    ) -> Result<Option<TradePlan>> {
        let context = self.build_planning_context_for_symbol(&plan.symbol)?;
        let planner = self.planner_for_symbol(&plan.symbol);
        let Some(intent) = planner.recovery_intent_from_result(plan, result, &context) else {
            return Ok(None);
        };
        let mut recovery_plan = planner.plan(&intent, &context).map_err(|rejection| {
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

    fn cex_market_order_with_options(
        &mut self,
        symbol: Symbol,
        side: OrderSide,
        base_qty: polyalpha_core::CexBaseQty,
        reduce_only: bool,
    ) -> OrderRequest {
        let (exchange, venue_symbol) = self.resolve_cex_target(&symbol);
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: self.next_client_order_id("plan-cex"),
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

    fn apply_fill_to_position(&mut self, fill: &Fill) {
        let effect = self.position_tracker.apply_fill(fill);

        if !effect.realized_pnl_delta.0.is_zero() {
            if let Some(session) = self.sessions.get_mut(&fill.symbol) {
                session.realized_pnl.0 += effect.realized_pnl_delta.0;
            }
        }
    }

    fn apply_execution_result_to_session(&mut self, symbol: &Symbol, result: &ExecutionResult) {
        if result.actual_funding_cost_usd.0.is_zero() {
            return;
        }

        let session = self
            .sessions
            .entry(symbol.clone())
            .or_insert_with(SessionBookkeeping::new);
        session.realized_pnl.0 -= result.actual_funding_cost_usd.0;
    }

    fn maybe_trade_closed_event(
        &mut self,
        symbol: &Symbol,
        result: &ExecutionResult,
    ) -> Option<ExecutionEvent> {
        if !self.position_tracker.symbol_is_flat(symbol) {
            return None;
        }
        if !self
            .position_tracker
            .positions_snapshot()
            .keys()
            .any(|key| &key.symbol == symbol)
        {
            return None;
        }

        let session = self.sessions.get_mut(symbol)?;
        let correlation_id = session
            .correlation_id
            .clone()
            .unwrap_or_else(|| result.correlation_id.clone());
        let realized_pnl = session.realized_pnl;
        *session = SessionBookkeeping::new();

        Some(ExecutionEvent::TradeClosed {
            symbol: symbol.clone(),
            correlation_id,
            realized_pnl,
            timestamp_ms: result.timestamp_ms,
        })
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
                let is_maker = req.post_only
                    || matches!(req.order_type, OrderType::Limit | OrderType::PostOnly);
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
                    fee: bps_fee(notional_usd, self.planner.poly_fee_bps),
                    is_maker,
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
                let is_maker = matches!(req.order_type, OrderType::Limit | OrderType::PostOnly);
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
                    fee: bps_fee(notional_usd, self.planner.cex_fee_bps(is_maker)),
                    is_maker,
                    timestamp_ms: response.timestamp_ms,
                }
            }
        }
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
    plan.post_rounding_residual_delta
        + (plan.cex_planned_qty.0 - filled_qty)
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

fn bps_fee(notional: UsdNotional, bps: u32) -> UsdNotional {
    UsdNotional(notional.0 * Decimal::from(bps) / Decimal::from(10_000u32))
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct ExecutionEconomics {
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

fn economics_for_notionals(
    reference: &TradePlan,
    planner: &ExecutionPlanner,
    poly_notional: UsdNotional,
    cex_notional: UsdNotional,
) -> ExecutionEconomics {
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

    ExecutionEconomics {
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

fn order_ledger_entry(
    request: &OrderRequest,
    response: &OrderResponse,
    fee_usd: UsdNotional,
) -> OrderLedgerEntry {
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
        fee_usd: fee_usd.0.to_f64().unwrap_or_default(),
        exchange_timestamp_ms: response.timestamp_ms,
        received_at_ms: response.timestamp_ms,
        status: format!("{:?}", response.status).to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use polyalpha_core::{
        CexBaseQty, ClientOrderId, CoreError, MarketConfig, OpenCandidate, OrderBookSnapshot,
        OrderExecutor, OrderId, OrderRequest, OrderResponse, OrderStatus, PlanningIntent,
        PolyOrderRequest, PolyShares, PolymarketIds, SignalStrength, VenueQuantity,
        PLANNING_SCHEMA_VERSION,
    };

    use crate::{
        dry_run::{DryRunExecutor, SlippageConfig},
        orderbook_provider::{InMemoryOrderbookProvider, OrderbookProvider},
    };

    use super::*;

    fn sample_symbol() -> Symbol {
        Symbol::new("btc-100k-mar-2026")
    }

    fn sample_market_with_step(
        exchange: Exchange,
        cex_symbol: &str,
        cex_qty_step: Decimal,
    ) -> MarketConfig {
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
            cex_qty_step,
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    fn sample_market(exchange: Exchange, cex_symbol: &str) -> MarketConfig {
        sample_market_with_step(exchange, cex_symbol, Decimal::new(1, 3))
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
                fair_value: 0.69,
                raw_mispricing: 0.20,
                delta_estimate: 0.00012,
                risk_budget_usd: 200.0,
                strength: SignalStrength::Strong,
                z_score: Some(2.8),
                raw_sigma: None,
                effective_sigma: None,
                sigma_source: None,
                returns_window_len: 0,
                timestamp_ms: 1_715_000_000_123,
            },
            max_budget_usd: 200.0,
            max_residual_delta: 0.05,
            max_shock_loss_usd: 200.0,
        }
    }

    fn sample_close_intent() -> PlanningIntent {
        PlanningIntent::ClosePosition {
            schema_version: PLANNING_SCHEMA_VERSION,
            intent_id: "intent-close-1".to_owned(),
            correlation_id: "corr-open-1".to_owned(),
            symbol: sample_symbol(),
            close_reason: "basis reverted".to_owned(),
            target_close_ratio: 1.0,
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

    fn sample_open_intent_with_residual_limit(max_residual_delta: f64) -> PlanningIntent {
        match sample_open_intent() {
            PlanningIntent::OpenPosition {
                schema_version,
                intent_id,
                correlation_id,
                symbol,
                candidate,
                max_budget_usd,
                max_shock_loss_usd,
                ..
            } => PlanningIntent::OpenPosition {
                schema_version,
                intent_id,
                correlation_id,
                symbol,
                candidate,
                max_budget_usd,
                max_residual_delta,
                max_shock_loss_usd,
            },
            _ => unreachable!("sample open intent must be open_position"),
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
        provider.update_cex(
            polyalpha_core::OrderBookSnapshot {
                exchange: Exchange::Okx,
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
            },
            "BTC-USDT-SWAP".to_owned(),
        );
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

    #[derive(Clone, Default)]
    struct RecoveryScriptExecutor {
        cex_attempts: Arc<AtomicUsize>,
    }

    impl RecoveryScriptExecutor {
        fn poly_fill_response(request: PolyOrderRequest) -> OrderResponse {
            OrderResponse {
                client_order_id: request.client_order_id,
                exchange_order_id: OrderId("poly-scripted-1".to_owned()),
                status: OrderStatus::Filled,
                filled_quantity: VenueQuantity::PolyShares(request.sizing.requested_shares()),
                average_price: Some(Price(Decimal::new(51, 2))),
                rejection_reason: None,
                timestamp_ms: 1,
            }
        }

        fn rejected_cex_response(client_order_id: ClientOrderId) -> OrderResponse {
            OrderResponse {
                client_order_id,
                exchange_order_id: OrderId("cex-scripted-reject".to_owned()),
                status: OrderStatus::Rejected,
                filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty::ZERO),
                average_price: None,
                rejection_reason: Some("scripted_cex_reject".to_owned()),
                timestamp_ms: 2,
            }
        }

        fn filled_cex_response(client_order_id: ClientOrderId, qty: CexBaseQty) -> OrderResponse {
            OrderResponse {
                client_order_id,
                exchange_order_id: OrderId("cex-scripted-fill".to_owned()),
                status: OrderStatus::Filled,
                filled_quantity: VenueQuantity::CexBaseQty(qty),
                average_price: Some(Price(Decimal::new(100_010, 0))),
                rejection_reason: None,
                timestamp_ms: 3,
            }
        }
    }

    #[async_trait]
    impl OrderExecutor for RecoveryScriptExecutor {
        async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
            match request {
                OrderRequest::Poly(req) => Ok(Self::poly_fill_response(req)),
                OrderRequest::Cex(req) => {
                    let attempt = self.cex_attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Ok(Self::rejected_cex_response(req.client_order_id))
                    } else {
                        Ok(Self::filled_cex_response(req.client_order_id, req.base_qty))
                    }
                }
            }
        }

        async fn cancel_order(&self, _exchange: Exchange, _order_id: &OrderId) -> Result<()> {
            Ok(())
        }

        async fn cancel_all(&self, _exchange: Exchange, _symbol: &Symbol) -> Result<u32> {
            Ok(0)
        }

        async fn query_order(
            &self,
            _exchange: Exchange,
            _order_id: &OrderId,
        ) -> Result<OrderResponse> {
            Err(CoreError::Channel(
                "query_order is not used in recovery tests".to_owned(),
            ))
        }
    }

    fn seed_multilevel_liquidity(provider: &InMemoryOrderbookProvider) {
        provider.update(polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: sample_symbol(),
            instrument: InstrumentKind::PolyYes,
            bids: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(49, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(48, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
            ],
            asks: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(51, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(52, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
            ],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });
        provider.update(polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: sample_symbol(),
            instrument: InstrumentKind::PolyNo,
            bids: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(49, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(48, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
            ],
            asks: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(51, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(52, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(100, 0))),
                },
            ],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });
        let cex_snapshot = polyalpha_core::OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: sample_symbol(),
            instrument: InstrumentKind::CexPerp,
            bids: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(100_000, 0)),
                    quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                        5, 0,
                    ))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(99_990, 0)),
                    quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                        5, 0,
                    ))),
                },
            ],
            asks: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(100_010, 0)),
                    quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                        5, 0,
                    ))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(100_020, 0)),
                    quantity: VenueQuantity::CexBaseQty(polyalpha_core::CexBaseQty(Decimal::new(
                        5, 0,
                    ))),
                },
            ],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        };
        provider.update_cex(cex_snapshot.clone(), "BTC100KMAR2026".to_owned());
        provider.update_cex(cex_snapshot, "BTCUSDT".to_owned());
    }

    struct RevalidatingProvider {
        plan_poly_yes: OrderBookSnapshot,
        live_poly_yes: OrderBookSnapshot,
        poly_no: OrderBookSnapshot,
        plan_cex: OrderBookSnapshot,
        live_cex: OrderBookSnapshot,
        poly_yes_reads: std::sync::atomic::AtomicUsize,
        cex_reads: std::sync::atomic::AtomicUsize,
    }

    impl RevalidatingProvider {
        fn new() -> Self {
            let provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
            let plan_poly_yes = provider
                .get_orderbook(
                    Exchange::Polymarket,
                    &sample_symbol(),
                    InstrumentKind::PolyYes,
                )
                .expect("poly yes book");
            let poly_no = provider
                .get_orderbook(
                    Exchange::Polymarket,
                    &sample_symbol(),
                    InstrumentKind::PolyNo,
                )
                .expect("poly no book");
            let plan_cex = provider
                .get_cex_orderbook(Exchange::Binance, "BTCUSDT")
                .expect("cex book");
            let mut live_poly_yes = plan_poly_yes.clone();
            live_poly_yes.sequence += 5;
            let mut live_cex = plan_cex.clone();
            live_cex.sequence += 5;
            Self {
                plan_poly_yes,
                live_poly_yes,
                poly_no,
                plan_cex,
                live_cex,
                poly_yes_reads: std::sync::atomic::AtomicUsize::new(0),
                cex_reads: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl OrderbookProvider for RevalidatingProvider {
        fn get_orderbook(
            &self,
            exchange: Exchange,
            symbol: &Symbol,
            instrument: InstrumentKind,
        ) -> Option<OrderBookSnapshot> {
            if exchange != Exchange::Polymarket || symbol != &sample_symbol() {
                return None;
            }
            match instrument {
                InstrumentKind::PolyYes => {
                    let reads = self
                        .poly_yes_reads
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if reads == 0 {
                        Some(self.plan_poly_yes.clone())
                    } else {
                        Some(self.live_poly_yes.clone())
                    }
                }
                InstrumentKind::PolyNo => Some(self.poly_no.clone()),
                InstrumentKind::CexPerp => None,
            }
        }

        fn get_cex_orderbook(
            &self,
            exchange: Exchange,
            venue_symbol: &str,
        ) -> Option<OrderBookSnapshot> {
            if exchange != Exchange::Binance
                || (venue_symbol != "BTCUSDT" && venue_symbol != "BTC100KMAR2026")
            {
                return None;
            }
            let reads = self
                .cex_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if reads == 0 {
                Some(self.plan_cex.clone())
            } else {
                Some(self.live_cex.clone())
            }
        }
    }

    #[tokio::test]
    async fn higher_priority_intent_supersedes_open_plan() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let open_plan = manager
            .plan_intent(&sample_open_intent())
            .expect("open intent should plan");
        manager.install_plan(open_plan, PlanLifecycleState::SubmittingPoly);
        let events = manager
            .process_intent(sample_force_exit_intent())
            .await
            .unwrap();

        assert!(events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::PlanSuperseded { .. })));
    }

    #[test]
    fn planning_context_respects_configured_depth_levels() {
        let provider = Arc::new(InMemoryOrderbookProvider::new());
        seed_multilevel_liquidity(&provider);
        let executor = DryRunExecutor::with_orderbook(provider.clone(), SlippageConfig::default());

        let mut default_manager =
            ExecutionManager::with_orderbook_provider(executor.clone(), provider.clone());
        let default_context = default_manager
            .build_planning_context_for_symbol(&sample_symbol())
            .expect("default planning context");
        assert_eq!(default_context.poly_yes_book.asks.len(), 1);

        let mut deep_manager = ExecutionManager::with_orderbook_provider(executor, provider)
            .with_planner_depth_levels(2);
        let deep_context = deep_manager
            .build_planning_context_for_symbol(&sample_symbol())
            .expect("depth-aware planning context");
        assert_eq!(deep_context.planner_depth_levels, 2);
        assert_eq!(deep_context.poly_yes_book.asks.len(), 2);
        assert_eq!(deep_context.cex_book.asks.len(), 2);
    }

    #[tokio::test]
    async fn lower_priority_intent_rejection_uses_machine_readable_reason_code() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let force_exit_plan = manager
            .plan_intent(&sample_force_exit_intent())
            .expect("force-exit intent should plan");
        manager.install_plan(force_exit_plan, PlanLifecycleState::PlanReady);
        let err = manager
            .process_intent(sample_open_intent())
            .await
            .expect_err("lower-priority plan must be rejected");

        assert!(err.to_string().contains("higher_priority_plan_active"));
    }

    #[tokio::test]
    async fn process_plan_rejects_external_open_plan_without_cex_hedge() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager =
            ExecutionManager::with_orderbook_provider(executor.clone(), planning_provider);
        let mut plan = manager
            .plan_intent(&sample_open_intent())
            .expect("sample open intent should plan");
        plan.cex_planned_qty = CexBaseQty::ZERO;

        let err = manager
            .process_plan(plan)
            .await
            .expect_err("external open plan without hedge must be rejected");

        assert!(err.to_string().contains("zero_cex_hedge_qty"));
        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        assert!(
            orders.is_empty(),
            "rejected external plan must not submit orders"
        );
    }

    #[tokio::test]
    async fn process_plan_executes_previewed_open_plan_with_same_plan_id() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(5, 0), Decimal::new(5, 0), true);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let plan = manager
            .preview_intent(&sample_open_intent())
            .expect("previewed open intent should plan");
        let previewed_plan_id = plan.plan_id.clone();

        let events = manager
            .process_plan(plan)
            .await
            .expect("previewed plan should execute");

        let emitted_plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_none() => {
                    Some(plan)
                }
                _ => None,
            })
            .expect("top-level trade plan should be emitted");
        assert_eq!(emitted_plan.plan_id, previewed_plan_id);
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
    async fn partial_poly_fill_rescales_child_plan_economics() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(5, 0), Decimal::new(5, 0), true);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();
        let parent_plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_none() => {
                    Some(plan.clone())
                }
                _ => None,
            })
            .expect("parent plan should be emitted");
        let child_plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradePlanCreated { plan }
                    if plan.parent_plan_id.as_deref() == Some(parent_plan.plan_id.as_str()) =>
                {
                    Some(plan.clone())
                }
                _ => None,
            })
            .expect("child hedge plan should be emitted");

        assert!(child_plan.poly_planned_shares.0 < parent_plan.poly_planned_shares.0);
        assert!(child_plan.poly_fee_usd.0 < parent_plan.poly_fee_usd.0);
        assert!(child_plan.cex_fee_usd.0 < parent_plan.cex_fee_usd.0);
        assert!(child_plan.planned_edge_usd.0 < parent_plan.planned_edge_usd.0);
    }

    #[tokio::test]
    async fn recovery_plan_is_emitted_when_cex_hedge_fails() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::ZERO, false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager
            .process_intent(sample_open_intent_with_residual_limit(0.01))
            .await
            .unwrap();

        assert!(events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::RecoveryPlanCreated { .. })));
    }

    #[tokio::test]
    async fn recovery_plan_prefers_cex_top_up_when_recovery_book_has_liquidity() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::ZERO, false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager
            .process_intent(sample_open_intent_with_residual_limit(0.01))
            .await
            .expect("open intent should emit recovery plan");
        let recovery_plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::RecoveryPlanCreated { plan } => Some(plan),
                _ => None,
            })
            .expect("recovery plan should exist");

        assert_eq!(recovery_plan.intent_type, "residual_recovery");
        assert_eq!(recovery_plan.poly_planned_shares, PolyShares::ZERO);
        assert!(recovery_plan.cex_planned_qty.0 > Decimal::ZERO);
    }

    #[tokio::test]
    async fn recovery_plan_is_executed_to_a_follow_up_result() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = RecoveryScriptExecutor::default();
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager
            .process_intent(sample_open_intent_with_residual_limit(0.01))
            .await
            .expect("open intent should drive recovery execution");

        let recovery_plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::RecoveryPlanCreated { plan } => Some(plan),
                _ => None,
            })
            .expect("recovery plan should be emitted");
        let recovery_result = events.iter().find_map(|event| match event {
            ExecutionEvent::ExecutionResultRecorded { result }
                if result.plan_id == recovery_plan.plan_id =>
            {
                Some(result)
            }
            _ => None,
        });

        assert!(
            recovery_result.is_some(),
            "recovery plan should be auto-submitted to a follow-up execution result"
        );
    }

    #[tokio::test]
    async fn process_intent_replans_against_revalidated_context_before_submit() {
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let provider = Arc::new(RevalidatingProvider::new());
        let mut manager = ExecutionManager::with_orderbook_provider(executor, provider);

        let events = manager
            .process_intent(sample_open_intent())
            .await
            .expect("intent should still succeed after replan");
        let plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_none() => {
                    Some(plan)
                }
                _ => None,
            })
            .expect("trade plan should be emitted");

        assert_eq!(plan.poly_sequence, 6);
        assert_eq!(plan.cex_sequence, 6);
    }

    #[tokio::test]
    async fn completed_open_intent_emits_execution_result() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();

        assert!(events.iter().any(|event| matches!(
            event,
            ExecutionEvent::ExecutionResultRecorded { result }
                if result.schema_version == PLANNING_SCHEMA_VERSION
                    && result.status == "completed"
                    && result.plan_id.starts_with("plan-")
        )));
    }

    #[tokio::test]
    async fn completed_plan_is_removed_from_active_set() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        manager
            .process_intent(sample_open_intent())
            .await
            .expect("open intent should complete");

        assert!(
            !manager.active_plans.contains_key(&sample_symbol()),
            "completed plans must be removed from the in-flight active set"
        );
    }

    #[tokio::test]
    async fn execution_result_records_actual_fee_breakdown() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();
        let result = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::ExecutionResultRecorded { result } => Some(result),
                _ => None,
            })
            .expect("execution result should be emitted");

        assert!(result.actual_poly_fee_usd.0 > Decimal::ZERO);
        assert!(result.actual_cex_fee_usd.0 > Decimal::ZERO);
        assert!(result.actual_funding_cost_usd.0 > Decimal::ZERO);
        let filled_fees = events
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::OrderFilled(fill) => Some(fill.fee.0),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            filled_fees.iter().all(|fee| *fee > Decimal::ZERO),
            "every emitted fill should carry its booked fee into downstream accounting"
        );
    }

    #[tokio::test]
    async fn planner_rejects_open_when_step_floor_pushes_residual_over_limit() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let registry = SymbolRegistry::new(vec![sample_market_with_step(
            Exchange::Binance,
            "BTCUSDT",
            Decimal::ONE,
        )]);
        let mut manager = ExecutionManager::with_symbol_registry_and_orderbook_provider(
            executor,
            registry,
            planning_provider,
        );

        let error = manager
            .process_intent(sample_open_intent_with_residual_limit(0.01))
            .await
            .expect_err("plan should be rejected once hedge is floored by venue step");

        let message = error.to_string();
        assert!(
            message.contains("zero_cex_hedge_qty"),
            "unexpected planner rejection: {message}"
        );
    }

    #[tokio::test]
    async fn created_open_plan_uses_rounded_cex_qty_and_post_rounding_residual() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let registry = SymbolRegistry::new(vec![sample_market_with_step(
            Exchange::Binance,
            "BTCUSDT",
            Decimal::new(1, 2),
        )]);
        let mut manager = ExecutionManager::with_symbol_registry_and_orderbook_provider(
            executor,
            registry,
            planning_provider,
        );

        let events = manager
            .process_intent(sample_open_intent())
            .await
            .expect("open intent should still produce a plan");
        let plan = events
            .iter()
            .find_map(|event| match event {
                ExecutionEvent::TradePlanCreated { plan } if plan.parent_plan_id.is_none() => {
                    Some(plan.clone())
                }
                _ => None,
            })
            .expect("plan should be emitted");

        let exact_qty = plan.poly_planned_shares.to_cex_base_qty(0.00012);
        let expected_qty = exact_qty.floor_to_step(Decimal::new(1, 2));
        let expected_residual = (exact_qty.0 - expected_qty.0).max(Decimal::ZERO);
        let expected_shock_2pct =
            UsdNotional(expected_residual * plan.cex_executable_price.0 * Decimal::new(2, 2));

        assert_eq!(plan.cex_planned_qty, expected_qty);
        assert!(
            (plan.post_rounding_residual_delta - expected_residual.to_f64().unwrap_or_default())
                .abs()
                < 1e-12,
            "plan residual delta should use rounded hedge qty"
        );
        assert_eq!(plan.shock_loss_up_2pct, expected_shock_2pct);
        assert_eq!(plan.shock_loss_down_2pct, expected_shock_2pct);
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
    async fn registry_backed_cex_orders_use_configured_symbol_and_exchange() {
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Binance, "BTCUSDT")]);
        let provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let mut manager = ExecutionManager::with_symbol_registry_and_orderbook_provider(
            executor.clone(),
            registry,
            provider,
        );

        manager
            .process_intent(sample_open_intent())
            .await
            .expect("open intent should process");

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
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Okx, "BTCUSDT")]);
        let provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let mut manager = ExecutionManager::with_symbol_registry_and_orderbook_provider(
            executor.clone(),
            registry,
            provider,
        );

        manager
            .process_intent(sample_open_intent())
            .await
            .expect("open intent should process");

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
    async fn close_position_intent_submits_flattening_orders() {
        let executor =
            sample_executor_with_liquidity(Decimal::new(500, 0), Decimal::new(5, 0), false);
        let registry = SymbolRegistry::new(vec![sample_market(Exchange::Binance, "BTCUSDT")]);
        let provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let mut manager = ExecutionManager::with_symbol_registry_and_orderbook_provider(
            executor.clone(),
            registry,
            provider,
        );

        manager
            .process_intent(sample_open_intent())
            .await
            .expect("open intent should process");

        let close_events = manager
            .process_intent(sample_close_intent())
            .await
            .expect("close intent should process");

        assert!(
            close_events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::TradePlanCreated { plan } if plan.intent_type == "close_position"))
        );
        assert!(
            close_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderSubmitted { .. }))
                .count()
                >= 2
        );
        assert!(
            close_events
                .iter()
                .filter(|event| matches!(event, ExecutionEvent::OrderFilled(_)))
                .count()
                >= 2
        );
        assert!(
            close_events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::TradeClosed { .. })),
            "flattening the position should emit a TradeClosed event once the symbol is flat"
        );

        let orders = executor
            .order_snapshots()
            .expect("dry run snapshot should succeed");
        assert_eq!(orders.len(), 4);
        let close_cex_order = orders
            .iter()
            .rev()
            .find(|order| order.exchange == Exchange::Binance)
            .expect("close cex order should exist");
        assert_eq!(close_cex_order.symbol.0, sample_symbol().0);
    }

    #[tokio::test]
    async fn rejected_open_without_fill_does_not_emit_trade_closed() {
        let planning_provider = sample_planning_provider(Decimal::new(500, 0), Decimal::new(5, 0));
        let executor = sample_executor_with_liquidity(Decimal::ZERO, Decimal::ZERO, false);
        let mut manager = ExecutionManager::with_orderbook_provider(executor, planning_provider);

        let events = manager.process_intent(sample_open_intent()).await.unwrap();

        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::TradeClosed { .. })),
            "a symbol that never filled should not produce TradeClosed"
        );
    }
}
