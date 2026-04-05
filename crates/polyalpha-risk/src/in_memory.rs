use std::collections::HashMap;

use async_trait::async_trait;
use rust_decimal::Decimal;

use polyalpha_core::{
    CexOrderRequest, CircuitBreakerStatus, Exchange, Fill, InstrumentKind, MarketPhase,
    OrderRequest, OrderSide, PolyOrderRequest, PolySizingInstruction, PositionKey, PositionTracker,
    RiskManager, RiskRejection, RiskStateSnapshot, Symbol, TokenSide, TradePlan, UsdNotional,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RiskLimits {
    pub max_total_exposure_usd: UsdNotional,
    pub max_single_position_usd: UsdNotional,
    pub max_daily_loss_usd: UsdNotional,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_total_exposure_usd: UsdNotional(Decimal::new(50_000, 0)),
            max_single_position_usd: UsdNotional(Decimal::new(10_000, 0)),
            max_daily_loss_usd: UsdNotional(Decimal::new(2_000, 0)),
        }
    }
}

impl From<polyalpha_core::RiskConfig> for RiskLimits {
    fn from(value: polyalpha_core::RiskConfig) -> Self {
        Self {
            max_total_exposure_usd: value.max_total_exposure_usd,
            max_single_position_usd: value.max_single_position_usd,
            max_daily_loss_usd: value.max_daily_loss_usd,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InMemoryRiskManager {
    limits: RiskLimits,
    breaker_status: CircuitBreakerStatus,
    breaker_reason: Option<String>,
    market_phases: HashMap<Symbol, MarketPhase>,
    position_tracker: PositionTracker,
    daily_realized_pnl: UsdNotional,
}

impl InMemoryRiskManager {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            breaker_status: CircuitBreakerStatus::Closed,
            breaker_reason: None,
            market_phases: HashMap::new(),
            position_tracker: PositionTracker::default(),
            daily_realized_pnl: UsdNotional::ZERO,
        }
    }

    pub fn limits(&self) -> &RiskLimits {
        &self.limits
    }

    pub fn position_tracker(&self) -> &PositionTracker {
        &self.position_tracker
    }

    pub fn update_market_phase(&mut self, symbol: Symbol, phase: MarketPhase) {
        self.market_phases.insert(symbol, phase);
    }

    pub fn build_snapshot(&self, timestamp_ms: u64) -> RiskStateSnapshot {
        RiskStateSnapshot {
            circuit_breaker: self.breaker_status,
            total_exposure_usd: self.position_tracker.total_exposure_usd(),
            positions: self.position_tracker.positions_snapshot(),
            daily_pnl: self.daily_realized_pnl,
            max_drawdown_pct: Decimal::ZERO,
            persistence_lag_secs: 0,
            timestamp_ms,
        }
    }

    pub fn breaker_reason(&self) -> Option<&str> {
        self.breaker_reason.as_deref()
    }

    pub fn market_phase(&self, symbol: &Symbol) -> Option<&MarketPhase> {
        self.market_phases.get(symbol)
    }

    /// Check if there's a CEX position for the given symbol
    pub fn has_cex_position(&self, symbol: &Symbol) -> bool {
        !self.cex_position_qty(symbol).is_zero()
    }

    /// Get CEX position quantity for a symbol (positive = long, negative = short)
    pub fn cex_position_qty(&self, symbol: &Symbol) -> Decimal {
        let key = PositionKey {
            exchange: Exchange::Binance, // Default to Binance
            symbol: symbol.clone(),
            instrument: InstrumentKind::CexPerp,
        };
        self.position_tracker.net_qty_for(&key)
    }

    pub fn reset_circuit_breaker(&mut self) {
        self.breaker_status = CircuitBreakerStatus::Closed;
        self.breaker_reason = None;
    }

    pub fn pre_trade_check_open_plan(&self, plan: &TradePlan) -> Result<(), RiskRejection> {
        if self.breaker_status == CircuitBreakerStatus::Open {
            return Err(RiskRejection::CircuitBreakerOpen);
        }

        self.check_open_plan_market_phase(plan)?;
        self.check_daily_loss_limit()?;
        // 开仓预算继续沿用 Poly 主腿口径，避免把 CEX 对冲腿也算进 200 USD
        // 单市场上限里，导致当前配置语义无意中整体漂移。
        self.check_exposure_limits(&plan.symbol, plan.poly_max_cost_usd)?;
        Ok(())
    }

    pub fn open_position_exposure_headroom_usd(&self, symbol: &Symbol) -> UsdNotional {
        let symbol_headroom = (self.limits.max_single_position_usd.0
            - self.position_tracker.symbol_exposure_usd(symbol).0)
            .max(Decimal::ZERO);
        let total_headroom = (self.limits.max_total_exposure_usd.0
            - self.position_tracker.total_exposure_usd().0)
            .max(Decimal::ZERO);
        UsdNotional(symbol_headroom.min(total_headroom))
    }

    fn check_market_phase(&self, request: &OrderRequest) -> Result<(), RiskRejection> {
        let symbol = request_symbol(request);
        let Some(phase) = self.market_phases.get(symbol) else {
            return Ok(());
        };

        if phase.allows_new_positions() || self.is_order_reducing_risk(request) {
            return Ok(());
        }

        Err(RiskRejection::MarketPhaseBlocked)
    }

    fn check_open_plan_market_phase(&self, plan: &TradePlan) -> Result<(), RiskRejection> {
        if plan.intent_type != "open_position" {
            return Ok(());
        }

        let Some(phase) = self.market_phases.get(&plan.symbol) else {
            return Ok(());
        };
        if phase.allows_new_positions() {
            return Ok(());
        }

        Err(RiskRejection::MarketPhaseBlocked)
    }

    fn check_daily_loss_limit(&self) -> Result<(), RiskRejection> {
        if self.daily_realized_pnl.0 <= -self.limits.max_daily_loss_usd.0 {
            return Err(RiskRejection::LimitBreached(
                "daily realized pnl is below max loss".to_owned(),
            ));
        }
        Ok(())
    }

    fn check_exposure_limits(
        &self,
        symbol: &Symbol,
        estimate: UsdNotional,
    ) -> Result<(), RiskRejection> {
        if estimate.0 <= Decimal::ZERO {
            return Ok(());
        }

        let projected_symbol = self.position_tracker.symbol_exposure_usd(symbol).0 + estimate.0;
        if projected_symbol > self.limits.max_single_position_usd.0 {
            return Err(RiskRejection::LimitBreached(format!(
                "single-position exposure {} > {}",
                projected_symbol, self.limits.max_single_position_usd.0
            )));
        }

        let projected_total = self.position_tracker.total_exposure_usd().0 + estimate.0;
        if projected_total > self.limits.max_total_exposure_usd.0 {
            return Err(RiskRejection::LimitBreached(format!(
                "total exposure {} > {}",
                projected_total, self.limits.max_total_exposure_usd.0
            )));
        }

        Ok(())
    }

    fn check_limits(&self, request: &OrderRequest) -> Result<(), RiskRejection> {
        self.check_daily_loss_limit()?;

        if !self.is_order_increasing_risk(request) {
            return Ok(());
        }

        let estimate = self.request_notional_estimate(request);
        self.check_exposure_limits(request_symbol(request), estimate)
    }

    fn request_notional_estimate(&self, request: &OrderRequest) -> UsdNotional {
        match request {
            OrderRequest::Poly(order) => match order.sizing {
                PolySizingInstruction::BuyBudgetCap { max_cost_usd, .. } => max_cost_usd,
                PolySizingInstruction::SellExactShares {
                    shares,
                    min_avg_price,
                } => {
                    if min_avg_price.0 > Decimal::ZERO {
                        UsdNotional(shares.0 * min_avg_price.0)
                    } else {
                        // 风控估值兜底：没有显式价格时回退到持仓均价，拿不到则按 0 处理。
                        let key = poly_position_key(order);
                        let fallback = self
                            .position_tracker
                            .entry_price_for(&key)
                            .unwrap_or(polyalpha_core::Price::ZERO);
                        UsdNotional(shares.0 * fallback.0)
                    }
                }
                PolySizingInstruction::SellMinProceeds {
                    min_proceeds_usd, ..
                } => min_proceeds_usd,
            },
            OrderRequest::Cex(order) => {
                if let Some(price) = order.price {
                    return UsdNotional(order.base_qty.0 * price.0);
                }

                // 风控估值兜底：没有显式价格时回退到持仓均价，拿不到则按 0 处理。
                let key = cex_position_key(order);
                let fallback = self
                    .position_tracker
                    .entry_price_for(&key)
                    .unwrap_or(polyalpha_core::Price::ZERO);
                UsdNotional(order.base_qty.0 * fallback.0)
            }
        }
    }

    fn is_order_increasing_risk(&self, request: &OrderRequest) -> bool {
        if let OrderRequest::Cex(order) = request {
            if order.reduce_only {
                return false;
            }
        }

        let Some(delta) = request_signed_qty(request) else {
            return true;
        };
        if delta.is_zero() {
            return false;
        }

        let key = request_position_key(request);
        let current = self.position_tracker.net_qty_for(&key);
        if current.is_zero() {
            return true;
        }
        if has_same_sign(current, delta) {
            return true;
        }
        delta.abs() > current.abs()
    }

    fn is_order_reducing_risk(&self, request: &OrderRequest) -> bool {
        if let OrderRequest::Cex(order) = request {
            if order.reduce_only {
                return true;
            }
        }

        let Some(delta) = request_signed_qty(request) else {
            return false;
        };
        if delta.is_zero() {
            return false;
        }

        let key = request_position_key(request);
        let current = self.position_tracker.net_qty_for(&key);
        if current.is_zero() {
            return false;
        }
        has_opposite_sign(current, delta) && delta.abs() <= current.abs()
    }
}

#[async_trait]
impl RiskManager for InMemoryRiskManager {
    async fn pre_trade_check(
        &self,
        request: OrderRequest,
    ) -> std::result::Result<OrderRequest, RiskRejection> {
        if self.breaker_status == CircuitBreakerStatus::Open {
            return Err(RiskRejection::CircuitBreakerOpen);
        }

        self.check_market_phase(&request)?;
        self.check_limits(&request)?;
        Ok(request)
    }

    fn open_position_exposure_headroom_usd(&self, symbol: &Symbol) -> UsdNotional {
        InMemoryRiskManager::open_position_exposure_headroom_usd(self, symbol)
    }

    fn pre_trade_check_open_plan(
        &self,
        plan: &TradePlan,
    ) -> std::result::Result<(), RiskRejection> {
        InMemoryRiskManager::pre_trade_check_open_plan(self, plan)
    }

    async fn on_fill(&mut self, fill: &Fill) -> polyalpha_core::Result<()> {
        let effect = self.position_tracker.apply_fill(fill);
        self.daily_realized_pnl.0 += effect.realized_pnl_delta.0;

        if self.daily_realized_pnl.0 <= -self.limits.max_daily_loss_usd.0 {
            self.trigger_circuit_breaker("daily loss limit reached");
        }
        Ok(())
    }

    async fn apply_realized_pnl_adjustment(
        &mut self,
        adjustment: UsdNotional,
    ) -> polyalpha_core::Result<()> {
        self.daily_realized_pnl.0 += adjustment.0;

        if self.daily_realized_pnl.0 <= -self.limits.max_daily_loss_usd.0 {
            self.trigger_circuit_breaker("daily loss limit reached");
        }
        Ok(())
    }

    fn circuit_breaker_status(&self) -> CircuitBreakerStatus {
        self.breaker_status
    }

    fn trigger_circuit_breaker(&mut self, reason: &str) {
        self.breaker_status = CircuitBreakerStatus::Open;
        self.breaker_reason = Some(reason.to_owned());
    }
}

fn request_symbol(request: &OrderRequest) -> &Symbol {
    match request {
        OrderRequest::Poly(order) => &order.symbol,
        OrderRequest::Cex(order) => &order.symbol,
    }
}

fn request_position_key(request: &OrderRequest) -> PositionKey {
    match request {
        OrderRequest::Poly(order) => poly_position_key(order),
        OrderRequest::Cex(order) => cex_position_key(order),
    }
}

fn poly_position_key(order: &PolyOrderRequest) -> PositionKey {
    PositionKey {
        exchange: Exchange::Polymarket,
        symbol: order.symbol.clone(),
        instrument: match order.token_side {
            TokenSide::Yes => InstrumentKind::PolyYes,
            TokenSide::No => InstrumentKind::PolyNo,
        },
    }
}

fn cex_position_key(order: &CexOrderRequest) -> PositionKey {
    PositionKey {
        exchange: order.exchange,
        symbol: order.symbol.clone(),
        instrument: InstrumentKind::CexPerp,
    }
}

fn request_signed_qty(request: &OrderRequest) -> Option<Decimal> {
    match request {
        OrderRequest::Poly(order) => Some(signed_by_side(
            order.side,
            order.sizing.requested_shares().0,
        )),
        OrderRequest::Cex(order) => Some(signed_by_side(order.side, order.base_qty.0)),
    }
}

fn signed_by_side(side: OrderSide, value: Decimal) -> Decimal {
    match side {
        OrderSide::Buy => value,
        OrderSide::Sell => -value,
    }
}

fn has_same_sign(lhs: Decimal, rhs: Decimal) -> bool {
    (lhs.is_sign_positive() && rhs.is_sign_positive())
        || (lhs.is_sign_negative() && rhs.is_sign_negative())
}

fn has_opposite_sign(lhs: Decimal, rhs: Decimal) -> bool {
    (lhs.is_sign_positive() && rhs.is_sign_negative())
        || (lhs.is_sign_negative() && rhs.is_sign_positive())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_core::{
        CexBaseQty, CexOrderRequest, ClientOrderId, Fill, OrderId, OrderType, PolyShares, Price,
        TimeInForce, VenueQuantity,
    };

    fn manager_with_limits(max_single: i64, max_total: i64, max_loss: i64) -> InMemoryRiskManager {
        InMemoryRiskManager::new(RiskLimits {
            max_total_exposure_usd: UsdNotional(Decimal::new(max_total, 0)),
            max_single_position_usd: UsdNotional(Decimal::new(max_single, 0)),
            max_daily_loss_usd: UsdNotional(Decimal::new(max_loss, 0)),
        })
    }

    fn cex_order(side: OrderSide, qty: i64, px: i64, reduce_only: bool) -> OrderRequest {
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId("cid-1".to_owned()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            venue_symbol: "BTCUSDT".to_owned(),
            side,
            order_type: OrderType::Limit,
            price: Some(Price(Decimal::new(px, 0))),
            base_qty: CexBaseQty(Decimal::new(qty, 0)),
            time_in_force: TimeInForce::Gtc,
            reduce_only,
        })
    }

    fn cex_fill(side: OrderSide, qty: i64, px: i64, fee: i64, ts: u64) -> Fill {
        Fill {
            fill_id: format!("fill-{ts}"),
            correlation_id: format!("corr-{ts}"),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            instrument: InstrumentKind::CexPerp,
            order_id: OrderId(format!("order-{ts}")),
            side,
            price: Price(Decimal::new(px, 0)),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(qty, 0))),
            notional_usd: UsdNotional(Decimal::new(px * qty, 0)),
            fee: UsdNotional(Decimal::new(fee, 0)),
            is_maker: false,
            timestamp_ms: ts,
        }
    }

    #[tokio::test]
    async fn pre_trade_rejects_when_breaker_is_open() {
        let mut manager = manager_with_limits(10_000, 50_000, 2_000);
        manager.trigger_circuit_breaker("manual");

        let result = manager
            .pre_trade_check(cex_order(OrderSide::Buy, 1, 100, false))
            .await;
        assert_eq!(result, Err(RiskRejection::CircuitBreakerOpen));
    }

    #[tokio::test]
    async fn pre_trade_blocks_new_risk_in_close_only_phase_but_allows_reducing() {
        let mut manager = manager_with_limits(10_000, 50_000, 2_000);
        let symbol = Symbol::new("btc-100k-mar-2026");
        manager.update_market_phase(
            symbol.clone(),
            MarketPhase::CloseOnly {
                hours_remaining: 1.0,
            },
        );

        let blocked = manager
            .pre_trade_check(cex_order(OrderSide::Buy, 1, 100, false))
            .await;
        assert_eq!(blocked, Err(RiskRejection::MarketPhaseBlocked));

        manager
            .on_fill(&cex_fill(OrderSide::Buy, 2, 100, 0, 1))
            .await
            .expect("fill should be applied");
        let reducing = manager
            .pre_trade_check(cex_order(OrderSide::Sell, 1, 100, false))
            .await;
        assert!(reducing.is_ok());
    }

    #[tokio::test]
    async fn on_fill_updates_snapshot_and_position_state() {
        let mut manager = manager_with_limits(10_000, 50_000, 2_000);
        manager
            .on_fill(&cex_fill(OrderSide::Buy, 2, 100, 1, 1))
            .await
            .expect("fill should be applied");

        let snapshot = manager.build_snapshot(123);
        assert_eq!(
            snapshot.total_exposure_usd,
            UsdNotional(Decimal::new(200, 0))
        );
        assert_eq!(snapshot.daily_pnl, UsdNotional(Decimal::new(-1, 0)));
        assert_eq!(snapshot.circuit_breaker, CircuitBreakerStatus::Closed);
        assert_eq!(snapshot.positions.len(), 1);
    }

    #[tokio::test]
    async fn realized_adjustments_update_daily_pnl_without_fill() {
        let mut manager = manager_with_limits(10_000, 50_000, 2_000);
        manager
            .apply_realized_pnl_adjustment(UsdNotional(Decimal::new(-42, 0)))
            .await
            .expect("adjustment should apply");

        let snapshot = manager.build_snapshot(456);
        assert_eq!(snapshot.daily_pnl, UsdNotional(Decimal::new(-42, 0)));
    }

    #[tokio::test]
    async fn pre_trade_rejects_when_exposure_limit_would_be_breached() {
        let manager = manager_with_limits(1_000, 5_000, 2_000);
        let result = manager
            .pre_trade_check(cex_order(OrderSide::Buy, 20, 100, false))
            .await;

        assert!(matches!(result, Err(RiskRejection::LimitBreached(_))));
    }

    #[test]
    fn risk_manager_trait_exposes_open_plan_gate() {
        fn assert_open_plan_gate<T: RiskManager>(risk: &T, plan: &TradePlan) {
            let _ = risk.pre_trade_check_open_plan(plan);
        }

        let manager = manager_with_limits(1_000, 5_000, 2_000);
        let plan = TradePlan {
            schema_version: polyalpha_core::PLANNING_SCHEMA_VERSION,
            plan_id: "plan-open-1".to_owned(),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: "idem-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            symbol: Symbol::new("btc-100k-mar-2026"),
            intent_type: "open_position".to_owned(),
            priority: "open_position".to_owned(),
            recovery_decision_reason: None,
            created_at_ms: 1,
            poly_exchange_timestamp_ms: 1,
            poly_received_at_ms: 1,
            poly_sequence: 1,
            cex_exchange_timestamp_ms: 1,
            cex_received_at_ms: 1,
            cex_sequence: 1,
            plan_hash: "hash-1".to_owned(),
            poly_side: OrderSide::Buy,
            poly_token_side: TokenSide::Yes,
            poly_sizing_mode: "buy_budget_cap".to_owned(),
            poly_requested_shares: PolyShares(Decimal::ONE),
            poly_planned_shares: PolyShares(Decimal::ONE),
            poly_max_cost_usd: UsdNotional(Decimal::new(100, 0)),
            poly_max_avg_price: Price::ONE,
            poly_max_shares: PolyShares(Decimal::ONE),
            poly_min_avg_price: Price::ZERO,
            poly_min_proceeds_usd: UsdNotional::ZERO,
            poly_book_avg_price: Price::ONE,
            poly_executable_price: Price::ONE,
            poly_friction_cost_usd: UsdNotional::ZERO,
            poly_fee_usd: UsdNotional::ZERO,
            cex_side: OrderSide::Sell,
            cex_planned_qty: CexBaseQty(Decimal::ONE),
            cex_book_avg_price: Price::ONE,
            cex_executable_price: Price::ONE,
            cex_friction_cost_usd: UsdNotional::ZERO,
            cex_fee_usd: UsdNotional::ZERO,
            raw_edge_usd: UsdNotional::ZERO,
            planned_edge_usd: UsdNotional::ZERO,
            expected_funding_cost_usd: UsdNotional::ZERO,
            residual_risk_penalty_usd: UsdNotional::ZERO,
            post_rounding_residual_delta: 0.0,
            shock_loss_up_1pct: UsdNotional::ZERO,
            shock_loss_down_1pct: UsdNotional::ZERO,
            shock_loss_up_2pct: UsdNotional::ZERO,
            shock_loss_down_2pct: UsdNotional::ZERO,
            plan_ttl_ms: 250,
            max_poly_sequence_drift: 1,
            max_cex_sequence_drift: 1,
            max_poly_price_move: Price::ZERO,
            max_cex_price_move: Price::ZERO,
            min_planned_edge_usd: UsdNotional::ZERO,
            max_residual_delta: 0.01,
            max_shock_loss_usd: UsdNotional::ZERO,
            max_plan_vs_fill_deviation_usd: UsdNotional::ZERO,
        };

        assert_open_plan_gate(&manager, &plan);
    }

    #[tokio::test]
    async fn daily_loss_triggers_circuit_breaker() {
        let mut manager = manager_with_limits(10_000, 50_000, 5);
        manager
            .on_fill(&cex_fill(OrderSide::Buy, 1, 100, 1, 1))
            .await
            .expect("entry fill should be applied");
        manager
            .on_fill(&cex_fill(OrderSide::Sell, 1, 90, 1, 2))
            .await
            .expect("exit fill should be applied");

        assert_eq!(manager.circuit_breaker_status(), CircuitBreakerStatus::Open);
        let check = manager
            .pre_trade_check(cex_order(OrderSide::Buy, 1, 100, false))
            .await;
        assert_eq!(check, Err(RiskRejection::CircuitBreakerOpen));
    }

    #[test]
    fn risk_limits_can_be_built_from_core_config() {
        let config = polyalpha_core::RiskConfig {
            max_total_exposure_usd: UsdNotional(Decimal::new(10, 0)),
            max_single_position_usd: UsdNotional(Decimal::new(5, 0)),
            max_daily_loss_usd: UsdNotional(Decimal::new(2, 0)),
            max_drawdown_pct: Decimal::new(5, 0),
            max_open_orders: 50,
            circuit_breaker_cooldown_secs: 10,
            rate_limit_orders_per_sec: 5,
            max_persistence_lag_secs: 5,
        };
        let limits = RiskLimits::from(config);
        assert_eq!(
            limits.max_total_exposure_usd,
            UsdNotional(Decimal::new(10, 0))
        );
        assert_eq!(
            limits.max_single_position_usd,
            UsdNotional(Decimal::new(5, 0))
        );
        assert_eq!(limits.max_daily_loss_usd, UsdNotional(Decimal::new(2, 0)));
    }

    #[tokio::test]
    async fn open_position_exposure_headroom_tracks_tighter_limit() {
        let mut manager = manager_with_limits(150, 220, 2_000);
        let symbol = Symbol::new("btc-100k-mar-2026");

        assert_eq!(
            manager.open_position_exposure_headroom_usd(&symbol),
            UsdNotional(Decimal::new(150, 0))
        );

        manager
            .on_fill(&cex_fill(OrderSide::Buy, 1, 100, 0, 1))
            .await
            .expect("fill should be applied");
        assert_eq!(
            manager.open_position_exposure_headroom_usd(&symbol),
            UsdNotional(Decimal::new(50, 0))
        );

        manager
            .on_fill(&cex_fill(OrderSide::Buy, 1, 50, 0, 2))
            .await
            .expect("second fill should be applied");
        assert_eq!(
            manager.open_position_exposure_headroom_usd(&symbol),
            UsdNotional::ZERO
        );
    }
}
