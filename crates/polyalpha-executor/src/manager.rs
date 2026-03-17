use std::collections::HashMap;

use uuid::Uuid;

use polyalpha_core::{
    ArbSignalAction, ArbSignalEvent, CexOrderRequest, ClientOrderId, DmmQuoteState,
    DmmQuoteUpdate, ExecutionEvent, Exchange, Fill, HedgeState, InstrumentKind, OrderExecutor,
    OrderId, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PolyOrderRequest,
    PolyShares, Price, Result, Symbol, TimeInForce, TokenSide, UsdNotional, VenueQuantity,
};

const DEFAULT_CEX_EXCHANGE: Exchange = Exchange::Binance;

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
}

impl SessionBookkeeping {
    fn new() -> Self {
        Self {
            session_id: Uuid::new_v4(),
            state: HedgeState::Idle,
            last_signal_id: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionManager<E: OrderExecutor> {
    executor: E,
    dmm_quote_states: HashMap<Symbol, DmmQuoteState>,
    active_dmm_orders: HashMap<Symbol, ActiveDmmOrders>,
    sessions: HashMap<Symbol, SessionBookkeeping>,
    next_client_order_seq: u64,
    next_fill_seq: u64,
}

impl<E: OrderExecutor> ExecutionManager<E> {
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            dmm_quote_states: HashMap::new(),
            active_dmm_orders: HashMap::new(),
            sessions: HashMap::new(),
            next_client_order_seq: 1,
            next_fill_seq: 1,
        }
    }

    pub fn dmm_quote_state(&self, symbol: &Symbol) -> Option<&DmmQuoteState> {
        self.dmm_quote_states.get(symbol)
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

                let bid_response = self.submit_order_with_events(
                    symbol.clone(),
                    Exchange::Polymarket,
                    bid_request,
                    &mut events,
                )
                .await?;
                let ask_response = self.submit_order_with_events(
                    symbol.clone(),
                    Exchange::Polymarket,
                    ask_request,
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
        let mut events = Vec::new();
        let symbol = signal.symbol.clone();
        let (session_id, mut current_state) = self.session_identity(&symbol);

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
                    &mut events,
                )
                .await?;
            if matches!(response.status, OrderStatus::Filled) {
                let fill = self.fill_from_request_and_response(
                    request,
                    response,
                    signal.timestamp_ms,
                );
                events.push(ExecutionEvent::OrderFilled(fill));
            }
        }

        let final_state = if matches!(signal.action, ArbSignalAction::ClosePosition { .. }) {
            HedgeState::Idle
        } else {
            HedgeState::Hedged
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
        out: &mut Vec<ExecutionEvent>,
    ) -> Result<OrderResponse> {
        let response = self.executor.submit_order(request).await?;
        out.push(ExecutionEvent::OrderSubmitted {
            symbol,
            exchange,
            response: response.clone(),
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
            limit_price: Some(price),
            shares: Some(quantity),
            quote_notional: None,
            time_in_force: TimeInForce::Gtc,
            post_only: true,
        })
    }

    fn requests_from_signal(&mut self, signal: &ArbSignalEvent) -> Vec<OrderRequest> {
        match &signal.action {
            ArbSignalAction::BasisLong {
                poly_side,
                poly_target_shares,
                poly_target_notional,
                cex_side,
                cex_hedge_qty,
                ..
            }
            | ArbSignalAction::BasisShort {
                poly_side,
                poly_target_shares,
                poly_target_notional,
                cex_side,
                cex_hedge_qty,
                ..
            } => vec![
                self.poly_market_order(
                    signal.symbol.clone(),
                    TokenSide::Yes,
                    *poly_side,
                    *poly_target_shares,
                    Some(*poly_target_notional),
                ),
                self.cex_market_order(signal.symbol.clone(), *cex_side, *cex_hedge_qty),
            ],
            ArbSignalAction::DeltaRebalance {
                cex_side,
                cex_qty_adjust,
                ..
            } => vec![self.cex_market_order(signal.symbol.clone(), *cex_side, *cex_qty_adjust)],
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
            ArbSignalAction::ClosePosition { .. } => Vec::new(),
        }
    }

    fn poly_market_order(
        &mut self,
        symbol: Symbol,
        token_side: TokenSide,
        side: OrderSide,
        shares: PolyShares,
        quote_notional: Option<UsdNotional>,
    ) -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: self.next_client_order_id("arb-poly"),
            symbol,
            token_side,
            side,
            order_type: OrderType::Market,
            limit_price: None,
            shares: Some(shares),
            quote_notional,
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
        let venue_symbol = Self::to_venue_symbol(&symbol);
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: self.next_client_order_id("arb-cex"),
            exchange: DEFAULT_CEX_EXCHANGE,
            symbol,
            venue_symbol,
            side,
            order_type: OrderType::Market,
            price: None,
            base_qty,
            time_in_force: TimeInForce::Ioc,
            reduce_only: false,
        })
    }

    fn to_venue_symbol(symbol: &Symbol) -> String {
        symbol.0.to_ascii_uppercase().replace('-', "")
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

    fn fill_from_request_and_response(
        &mut self,
        request: OrderRequest,
        response: OrderResponse,
        timestamp_ms: u64,
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
                    timestamp_ms,
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
                    timestamp_ms,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{ArbSignalAction, SignalStrength};

    use crate::dry_run::DryRunExecutor;

    use super::*;

    fn sample_symbol() -> Symbol {
        Symbol::new("btc-100k-mar-2026")
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
        let executor = DryRunExecutor::new();
        let mut manager = ExecutionManager::new(executor);
        let signal = ArbSignalEvent {
            signal_id: "sig-basis-1".to_owned(),
            symbol: sample_symbol(),
            action: ArbSignalAction::BasisLong {
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
        };

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
}
