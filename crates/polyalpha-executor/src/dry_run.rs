use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use polyalpha_core::{
    CexBaseQty, CoreError, Exchange, InstrumentKind, OrderBookSnapshot, OrderExecutor, OrderId,
    OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PolyShares,
    PolySizingInstruction, Price, Result, Symbol, TimeInForce, UsdNotional, VenueQuantity,
};

use crate::orderbook_provider::OrderbookProvider;

const DEFAULT_START_TS_MS: u64 = 1_700_000_000_000;

/// Configuration for slippage and liquidity simulation.
#[derive(Clone, Debug)]
pub struct SlippageConfig {
    /// Additional slippage for Polymarket orders (in basis points).
    /// E.g., 50 means 0.5% extra slippage on top of orderbook spread.
    pub poly_slippage_bps: u64,
    /// Additional slippage for CEX orders (in basis points).
    pub cex_slippage_bps: u64,
    /// Minimum liquidity required (in shares for Poly, base qty for CEX).
    /// Orders with less available liquidity will be rejected.
    pub min_liquidity: Decimal,
    /// Whether to allow partial fills when liquidity is insufficient.
    pub allow_partial_fill: bool,
}

impl Default for SlippageConfig {
    fn default() -> Self {
        Self {
            poly_slippage_bps: 50,
            cex_slippage_bps: 2,
            min_liquidity: Decimal::new(100, 0), // 100 shares minimum
            allow_partial_fill: false,
        }
    }
}

/// Result of fill price calculation from orderbook.
#[derive(Clone, Debug)]
pub struct FillEstimation {
    /// Average fill price across all consumed levels.
    pub average_price: Price,
    /// Quantity that can be filled.
    pub filled_quantity: Decimal,
    /// Quantity that cannot be filled due to insufficient liquidity.
    pub unfilled_quantity: Decimal,
    /// Whether the order can be completely filled.
    pub is_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone)]
pub struct DryRunExecutor {
    orders: Arc<Mutex<HashMap<OrderId, StoredOrder>>>,
    next_order_seq: Arc<AtomicU64>,
    next_timestamp_ms: Arc<AtomicU64>,
    /// Optional orderbook provider for realistic fill simulation.
    orderbook_provider: Option<Arc<dyn OrderbookProvider>>,
    /// Slippage configuration for fill simulation.
    slippage_config: SlippageConfig,
}

impl std::fmt::Debug for DryRunExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DryRunExecutor")
            .field("next_order_seq", &self.next_order_seq)
            .field("next_timestamp_ms", &self.next_timestamp_ms)
            .field("slippage_config", &self.slippage_config)
            .field("has_orderbook_provider", &self.orderbook_provider.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
struct StoredOrder {
    exchange: Exchange,
    symbol: Symbol,
    venue_symbol: Option<String>,
    side: OrderSide,
    response: OrderResponse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DryRunOrderSnapshot {
    pub exchange_order_id: OrderId,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub venue_symbol: Option<String>,
    pub status: OrderStatus,
    pub timestamp_ms: u64,
}

impl Default for DryRunExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DryRunExecutor {
    /// Create a new DryRunExecutor without orderbook simulation.
    pub fn new() -> Self {
        Self::with_start_timestamp(DEFAULT_START_TS_MS)
    }

    /// Create a DryRunExecutor with a custom start timestamp.
    pub fn with_start_timestamp(start_timestamp_ms: u64) -> Self {
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            next_order_seq: Arc::new(AtomicU64::new(1)),
            next_timestamp_ms: Arc::new(AtomicU64::new(start_timestamp_ms)),
            orderbook_provider: None,
            slippage_config: SlippageConfig::default(),
        }
    }

    /// Create a DryRunExecutor with orderbook-based fill simulation.
    pub fn with_orderbook(
        orderbook_provider: Arc<dyn OrderbookProvider>,
        slippage_config: SlippageConfig,
    ) -> Self {
        Self::with_orderbook_and_start_timestamp(
            orderbook_provider,
            slippage_config,
            DEFAULT_START_TS_MS,
        )
    }

    /// Create a DryRunExecutor with orderbook simulation and a custom start timestamp.
    pub fn with_orderbook_and_start_timestamp(
        orderbook_provider: Arc<dyn OrderbookProvider>,
        slippage_config: SlippageConfig,
        start_timestamp_ms: u64,
    ) -> Self {
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            next_order_seq: Arc::new(AtomicU64::new(1)),
            next_timestamp_ms: Arc::new(AtomicU64::new(start_timestamp_ms)),
            orderbook_provider: Some(orderbook_provider),
            slippage_config,
        }
    }

    /// Update slippage configuration.
    pub fn set_slippage_config(&mut self, config: SlippageConfig) {
        self.slippage_config = config;
    }

    /// Calculate fill price from orderbook depth.
    fn calculate_fill_price(
        &self,
        orderbook: &OrderBookSnapshot,
        side: OrderSide,
        requested_qty: Decimal,
    ) -> FillEstimation {
        let mut levels = match side {
            OrderSide::Buy => orderbook.asks.clone(), // Buy consumes asks
            OrderSide::Sell => orderbook.bids.clone(), // Sell consumes bids
        };
        match side {
            OrderSide::Buy => levels.sort_by(|left, right| left.price.cmp(&right.price)),
            OrderSide::Sell => levels.sort_by(|left, right| right.price.cmp(&left.price)),
        }

        let mut remaining = requested_qty;
        let mut total_cost = Decimal::ZERO;
        let mut filled = Decimal::ZERO;

        for level in &levels {
            let qty_at_level = Self::extract_qty(&level.quantity);
            let fill_qty = remaining.min(qty_at_level);

            total_cost += fill_qty * level.price.0;
            filled += fill_qty;
            remaining -= fill_qty;

            if remaining.is_zero() {
                break;
            }
        }

        let avg_price = if filled.is_zero() {
            // Fallback to mid price if no liquidity
            Self::mid_price(orderbook).unwrap_or(Price::ONE)
        } else {
            Price(total_cost / filled)
        };

        FillEstimation {
            average_price: avg_price,
            filled_quantity: filled,
            unfilled_quantity: remaining,
            is_complete: remaining.is_zero(),
        }
    }

    /// Extract decimal quantity from VenueQuantity.
    fn extract_qty(qty: &VenueQuantity) -> Decimal {
        match qty {
            VenueQuantity::PolyShares(shares) => shares.0,
            VenueQuantity::CexBaseQty(base) => base.0,
        }
    }

    /// Calculate mid price from orderbook.
    fn mid_price(orderbook: &OrderBookSnapshot) -> Option<Price> {
        let best_bid = orderbook
            .bids
            .iter()
            .max_by(|left, right| left.price.cmp(&right.price))?;
        let best_ask = orderbook
            .asks
            .iter()
            .min_by(|left, right| left.price.cmp(&right.price))?;
        let mid = (best_bid.price.0 + best_ask.price.0) / Decimal::from(2);
        Some(Price(mid))
    }

    /// Apply slippage to the fill price.
    fn apply_slippage(&self, price: Price, side: OrderSide, is_poly: bool) -> Price {
        let bps = if is_poly {
            Decimal::from(self.slippage_config.poly_slippage_bps)
        } else {
            Decimal::from(self.slippage_config.cex_slippage_bps)
        };
        let slippage_multiplier = bps / Decimal::from(10_000);

        let slipped = match side {
            OrderSide::Buy => {
                // Buying: price goes up
                Price(price.0 * (Decimal::ONE + slippage_multiplier))
            }
            OrderSide::Sell => {
                // Selling: price goes down
                Price(price.0 * (Decimal::ONE - slippage_multiplier))
            }
        };

        if is_poly {
            Price(slipped.0.clamp(Decimal::ZERO, Decimal::ONE))
        } else {
            slipped
        }
    }

    /// Get orderbook for a request, if available.
    fn get_orderbook_for_request(&self, request: &OrderRequest) -> Option<OrderBookSnapshot> {
        let provider = self.orderbook_provider.as_ref()?;

        match request {
            OrderRequest::Poly(req) => {
                let instrument = match req.token_side {
                    polyalpha_core::TokenSide::Yes => InstrumentKind::PolyYes,
                    polyalpha_core::TokenSide::No => InstrumentKind::PolyNo,
                };
                provider.get_orderbook(Exchange::Polymarket, &req.symbol, instrument)
            }
            OrderRequest::Cex(req) => {
                // For CEX orders, use venue_symbol (e.g., "BTCUSDT") to look up the orderbook
                // This works because CEX orderbooks are stored by venue_symbol
                provider.get_cex_orderbook(req.exchange, &req.venue_symbol)
            }
        }
    }

    fn request_context(request: &OrderRequest) -> (Exchange, Symbol, InstrumentKind, OrderSide) {
        match request {
            OrderRequest::Poly(req) => (
                Exchange::Polymarket,
                req.symbol.clone(),
                match req.token_side {
                    polyalpha_core::TokenSide::Yes => InstrumentKind::PolyYes,
                    polyalpha_core::TokenSide::No => InstrumentKind::PolyNo,
                },
                req.side,
            ),
            OrderRequest::Cex(req) => (
                req.exchange,
                req.symbol.clone(),
                InstrumentKind::CexPerp,
                req.side,
            ),
        }
    }

    fn friction_cost_usd(
        side: OrderSide,
        orderbook_mid_price: Option<Price>,
        executable_price: Option<Price>,
        filled_quantity: Decimal,
    ) -> Option<UsdNotional> {
        let mid = orderbook_mid_price?;
        let exec = executable_price?;
        let mid_notional = mid.0 * filled_quantity;
        let exec_notional = exec.0 * filled_quantity;
        let cost = match side {
            OrderSide::Buy => (exec_notional - mid_notional).max(Decimal::ZERO),
            OrderSide::Sell => (mid_notional - exec_notional).max(Decimal::ZERO),
        };
        Some(UsdNotional(cost))
    }

    fn rejected_estimate(
        request: &OrderRequest,
        orderbook_mid_price: Option<Price>,
        rejection_reason: &str,
    ) -> OrderExecutionEstimate {
        let (exchange, symbol, instrument, side) = Self::request_context(request);
        OrderExecutionEstimate {
            exchange,
            symbol,
            instrument,
            side,
            status: OrderStatus::Rejected,
            requested_quantity: Self::requested_quantity(request),
            filled_quantity: Self::zero_quantity_for_request(request),
            orderbook_mid_price,
            book_average_price: None,
            executable_price: None,
            executable_notional_usd: UsdNotional::ZERO,
            friction_cost_usd: None,
            fee_usd: None,
            rejection_reason: Some(rejection_reason.to_owned()),
            is_complete: false,
        }
    }

    fn open_estimate(
        request: &OrderRequest,
        orderbook_mid_price: Option<Price>,
    ) -> OrderExecutionEstimate {
        let (exchange, symbol, instrument, side) = Self::request_context(request);
        OrderExecutionEstimate {
            exchange,
            symbol,
            instrument,
            side,
            status: OrderStatus::Open,
            requested_quantity: Self::requested_quantity(request),
            filled_quantity: Self::zero_quantity_for_request(request),
            orderbook_mid_price,
            book_average_price: None,
            executable_price: Self::initial_price(request),
            executable_notional_usd: UsdNotional::ZERO,
            friction_cost_usd: None,
            fee_usd: Some(UsdNotional::ZERO),
            rejection_reason: None,
            is_complete: false,
        }
    }

    fn poly_buy_budget_outcome(
        &self,
        request: &OrderRequest,
        orderbook: &OrderBookSnapshot,
        max_cost_usd: UsdNotional,
        max_avg_price: Price,
        max_shares: PolyShares,
    ) -> OrderExecutionEstimate {
        let mut asks = orderbook.asks.clone();
        asks.sort_by(|left, right| left.price.cmp(&right.price));

        let mut filled = Decimal::ZERO;
        let mut book_cost = Decimal::ZERO;
        for level in &asks {
            if filled >= max_shares.0 {
                break;
            }

            let price = level.price.0;
            let available = Self::extract_qty(&level.quantity);
            if available <= Decimal::ZERO || price <= Decimal::ZERO {
                continue;
            }

            let max_fill_here = available.min((max_shares.0 - filled).max(Decimal::ZERO));
            let can_take_full_level = {
                let next_filled = filled + max_fill_here;
                let next_book_cost = book_cost + max_fill_here * price;
                let next_book_avg = Price(next_book_cost / next_filled);
                let next_exec_price = self.apply_slippage(next_book_avg, OrderSide::Buy, true);
                next_exec_price.0 <= max_avg_price.0
                    && next_filled * next_exec_price.0 <= max_cost_usd.0
            };

            let fill_qty = if can_take_full_level {
                max_fill_here
            } else {
                let mut low = Decimal::ZERO;
                let mut high = max_fill_here;
                for _ in 0..48 {
                    let mid = (low + high) / Decimal::from(2);
                    if mid <= Decimal::ZERO {
                        break;
                    }

                    let next_filled = filled + mid;
                    let next_book_cost = book_cost + mid * price;
                    let next_book_avg = Price(next_book_cost / next_filled);
                    let next_exec_price = self.apply_slippage(next_book_avg, OrderSide::Buy, true);
                    let within_constraints = next_exec_price.0 <= max_avg_price.0
                        && next_filled * next_exec_price.0 <= max_cost_usd.0;
                    if within_constraints {
                        low = mid;
                    } else {
                        high = mid;
                    }
                }
                low
            };

            if fill_qty <= Decimal::ZERO {
                if filled <= Decimal::ZERO {
                    break;
                }
                continue;
            }

            filled += fill_qty;
            book_cost += fill_qty * price;
        }

        if filled <= Decimal::ZERO {
            let rejection_reason = if asks.is_empty() {
                "insufficient_poly_depth"
            } else {
                "poly_max_price_exceeded"
            };
            return Self::rejected_estimate(request, Self::mid_price(orderbook), rejection_reason);
        }

        let book_average_price = Price(book_cost / filled);
        let executable_price = self.apply_slippage(book_average_price, OrderSide::Buy, true);
        let requested_quantity = Self::requested_quantity(request);
        let filled_quantity = VenueQuantity::PolyShares(PolyShares(filled));
        let orderbook_mid_price = Self::mid_price(orderbook);

        OrderExecutionEstimate {
            exchange: Exchange::Polymarket,
            symbol: match request {
                OrderRequest::Poly(req) => req.symbol.clone(),
                OrderRequest::Cex(_) => unreachable!("poly estimate requires poly request"),
            },
            instrument: match request {
                OrderRequest::Poly(req) => match req.token_side {
                    polyalpha_core::TokenSide::Yes => InstrumentKind::PolyYes,
                    polyalpha_core::TokenSide::No => InstrumentKind::PolyNo,
                },
                OrderRequest::Cex(_) => unreachable!("poly estimate requires poly request"),
            },
            side: OrderSide::Buy,
            status: OrderStatus::Filled,
            requested_quantity,
            filled_quantity,
            orderbook_mid_price,
            book_average_price: Some(book_average_price),
            executable_price: Some(executable_price),
            executable_notional_usd: UsdNotional(filled * executable_price.0),
            friction_cost_usd: Self::friction_cost_usd(
                OrderSide::Buy,
                orderbook_mid_price,
                Some(executable_price),
                filled,
            ),
            fee_usd: Some(UsdNotional::ZERO),
            rejection_reason: None,
            is_complete: true,
        }
    }

    fn poly_sell_outcome(
        &self,
        request: &OrderRequest,
        orderbook: &OrderBookSnapshot,
        shares: PolyShares,
        min_avg_price: Option<Price>,
        min_proceeds_usd: Option<UsdNotional>,
    ) -> OrderExecutionEstimate {
        let fill = self.calculate_fill_price(orderbook, OrderSide::Sell, shares.0);
        if !fill.is_complete && !self.slippage_config.allow_partial_fill {
            return Self::rejected_estimate(
                request,
                Self::mid_price(orderbook),
                "insufficient_poly_depth",
            );
        }
        if fill.filled_quantity <= Decimal::ZERO {
            return Self::rejected_estimate(
                request,
                Self::mid_price(orderbook),
                "insufficient_poly_depth",
            );
        }

        let executable_price = self.apply_slippage(fill.average_price, OrderSide::Sell, true);
        let executable_notional_usd = UsdNotional(fill.filled_quantity * executable_price.0);
        if let Some(min_avg_price) = min_avg_price {
            if executable_price.0 < min_avg_price.0 {
                return Self::rejected_estimate(
                    request,
                    Self::mid_price(orderbook),
                    "poly_min_proceeds_not_met",
                );
            }
        }
        if let Some(min_proceeds_usd) = min_proceeds_usd {
            if executable_notional_usd.0 < min_proceeds_usd.0 {
                return Self::rejected_estimate(
                    request,
                    Self::mid_price(orderbook),
                    "poly_min_proceeds_not_met",
                );
            }
        }

        let filled_quantity = VenueQuantity::PolyShares(PolyShares(fill.filled_quantity));
        let orderbook_mid_price = Self::mid_price(orderbook);
        OrderExecutionEstimate {
            exchange: Exchange::Polymarket,
            symbol: match request {
                OrderRequest::Poly(req) => req.symbol.clone(),
                OrderRequest::Cex(_) => unreachable!("poly estimate requires poly request"),
            },
            instrument: match request {
                OrderRequest::Poly(req) => match req.token_side {
                    polyalpha_core::TokenSide::Yes => InstrumentKind::PolyYes,
                    polyalpha_core::TokenSide::No => InstrumentKind::PolyNo,
                },
                OrderRequest::Cex(_) => unreachable!("poly estimate requires poly request"),
            },
            side: OrderSide::Sell,
            status: if fill.is_complete {
                OrderStatus::Filled
            } else {
                OrderStatus::PartialFill
            },
            requested_quantity: Self::requested_quantity(request),
            filled_quantity,
            orderbook_mid_price,
            book_average_price: Some(fill.average_price),
            executable_price: Some(executable_price),
            executable_notional_usd,
            friction_cost_usd: Self::friction_cost_usd(
                OrderSide::Sell,
                orderbook_mid_price,
                Some(executable_price),
                fill.filled_quantity,
            ),
            fee_usd: Some(UsdNotional::ZERO),
            rejection_reason: None,
            is_complete: fill.is_complete,
        }
    }

    pub fn estimate_order_request(&self, request: &OrderRequest) -> OrderExecutionEstimate {
        let requested_quantity = Self::requested_quantity(request);
        if Self::extract_qty(&requested_quantity) <= Decimal::ZERO {
            return Self::rejected_estimate(request, None, "zero_quantity");
        }

        let orderbook = self.get_orderbook_for_request(request);
        let orderbook_mid_price = orderbook.as_ref().and_then(Self::mid_price);

        if Self::is_gtc_limit(request) {
            return Self::open_estimate(request, orderbook_mid_price);
        }

        let Some(orderbook) = orderbook else {
            return Self::rejected_estimate(request, None, "no_orderbook_for_fill");
        };

        let available_liquidity: Decimal = {
            let (_, _, _, side) = Self::request_context(request);
            let levels = match side {
                OrderSide::Buy => &orderbook.asks,
                OrderSide::Sell => &orderbook.bids,
            };
            levels
                .iter()
                .map(|level| Self::extract_qty(&level.quantity))
                .sum()
        };

        if matches!(request, OrderRequest::Poly(_))
            && available_liquidity < self.slippage_config.min_liquidity
        {
            return Self::rejected_estimate(request, orderbook_mid_price, "insufficient_liquidity");
        }

        match request {
            OrderRequest::Poly(req) => match req.sizing {
                PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd,
                    max_avg_price,
                    max_shares,
                } => self.poly_buy_budget_outcome(
                    request,
                    &orderbook,
                    max_cost_usd,
                    max_avg_price,
                    max_shares,
                ),
                PolySizingInstruction::SellExactShares {
                    shares,
                    min_avg_price,
                } => self.poly_sell_outcome(request, &orderbook, shares, Some(min_avg_price), None),
                PolySizingInstruction::SellMinProceeds {
                    shares,
                    min_proceeds_usd,
                } => self.poly_sell_outcome(
                    request,
                    &orderbook,
                    shares,
                    None,
                    Some(min_proceeds_usd),
                ),
            },
            OrderRequest::Cex(req) => {
                let fill = self.calculate_fill_price(&orderbook, req.side, req.base_qty.0);
                if !fill.is_complete && !self.slippage_config.allow_partial_fill {
                    return Self::rejected_estimate(
                        request,
                        orderbook_mid_price,
                        "partial_fill_not_allowed",
                    );
                }

                let executable_price = self.apply_slippage(fill.average_price, req.side, false);
                let filled_qty = if fill.is_complete {
                    VenueQuantity::CexBaseQty(req.base_qty)
                } else {
                    VenueQuantity::CexBaseQty(CexBaseQty(fill.filled_quantity))
                };

                OrderExecutionEstimate {
                    exchange: req.exchange,
                    symbol: req.symbol.clone(),
                    instrument: InstrumentKind::CexPerp,
                    side: req.side,
                    status: if fill.is_complete {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::PartialFill
                    },
                    requested_quantity,
                    filled_quantity: filled_qty,
                    orderbook_mid_price,
                    book_average_price: Some(fill.average_price),
                    executable_price: Some(executable_price),
                    executable_notional_usd: UsdNotional(fill.filled_quantity * executable_price.0),
                    friction_cost_usd: Self::friction_cost_usd(
                        req.side,
                        orderbook_mid_price,
                        Some(executable_price),
                        fill.filled_quantity,
                    ),
                    fee_usd: Some(UsdNotional::ZERO),
                    rejection_reason: None,
                    is_complete: fill.is_complete,
                }
            }
        }
    }

    /// Determine order status and fill details based on orderbook.
    fn determine_fill(
        &self,
        request: &OrderRequest,
    ) -> (OrderStatus, VenueQuantity, Option<Price>, Option<String>) {
        let estimate = self.estimate_order_request(request);
        (
            estimate.status,
            estimate.filled_quantity,
            estimate.executable_price,
            estimate.rejection_reason,
        )
    }

    /// Check if request is a GTC limit order.
    fn is_gtc_limit(request: &OrderRequest) -> bool {
        match request {
            OrderRequest::Poly(req) => {
                req.post_only
                    || (matches!(req.order_type, OrderType::Limit | OrderType::PostOnly)
                        && matches!(req.time_in_force, TimeInForce::Gtc))
            }
            OrderRequest::Cex(req) => {
                matches!(req.order_type, OrderType::Limit | OrderType::PostOnly)
                    && matches!(req.time_in_force, TimeInForce::Gtc)
            }
        }
    }

    fn next_order_id(&self) -> OrderId {
        let seq = self.next_order_seq.fetch_add(1, Ordering::Relaxed);
        OrderId(format!("dry-order-{seq}"))
    }

    fn next_timestamp_ms(&self) -> u64 {
        self.next_timestamp_ms.fetch_add(1, Ordering::Relaxed)
    }

    fn zero_quantity_for_request(request: &OrderRequest) -> VenueQuantity {
        match request {
            OrderRequest::Poly(_) => VenueQuantity::PolyShares(PolyShares::ZERO),
            OrderRequest::Cex(_) => VenueQuantity::CexBaseQty(CexBaseQty::default()),
        }
    }

    fn requested_quantity(request: &OrderRequest) -> VenueQuantity {
        match request {
            OrderRequest::Poly(req) => VenueQuantity::PolyShares(req.sizing.requested_shares()),
            OrderRequest::Cex(req) => VenueQuantity::CexBaseQty(req.base_qty),
        }
    }

    fn order_identity(request: &OrderRequest) -> (Exchange, Symbol, Option<String>) {
        match request {
            OrderRequest::Poly(req) => (Exchange::Polymarket, req.symbol.clone(), None),
            OrderRequest::Cex(req) => (
                req.exchange,
                req.symbol.clone(),
                Some(req.venue_symbol.clone()),
            ),
        }
    }

    fn order_side(request: &OrderRequest) -> OrderSide {
        match request {
            OrderRequest::Poly(req) => req.side,
            OrderRequest::Cex(req) => req.side,
        }
    }

    fn initial_price(request: &OrderRequest) -> Option<Price> {
        match request {
            OrderRequest::Poly(req) => req.sizing.boundary_price(),
            OrderRequest::Cex(req) => req.price.or(Some(Price::ONE)),
        }
    }

    fn is_cancellable(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartialFill
        )
    }

    fn to_snapshot(order_id: &OrderId, stored: &StoredOrder) -> DryRunOrderSnapshot {
        DryRunOrderSnapshot {
            exchange_order_id: order_id.clone(),
            exchange: stored.exchange,
            symbol: stored.symbol.clone(),
            venue_symbol: stored.venue_symbol.clone(),
            status: stored.response.status,
            timestamp_ms: stored.response.timestamp_ms,
        }
    }

    pub fn order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        let orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;
        let mut out: Vec<DryRunOrderSnapshot> = orders
            .iter()
            .map(|(order_id, stored)| Self::to_snapshot(order_id, stored))
            .collect();
        out.sort_by(|lhs, rhs| lhs.exchange_order_id.0.cmp(&rhs.exchange_order_id.0));
        Ok(out)
    }

    pub fn open_order_snapshots(&self) -> Result<Vec<DryRunOrderSnapshot>> {
        let orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;
        let mut out: Vec<DryRunOrderSnapshot> = orders
            .iter()
            .filter_map(|(order_id, stored)| {
                if Self::is_cancellable(stored.response.status) {
                    Some(Self::to_snapshot(order_id, stored))
                } else {
                    None
                }
            })
            .collect();
        out.sort_by(|lhs, rhs| lhs.exchange_order_id.0.cmp(&rhs.exchange_order_id.0));
        Ok(out)
    }

    pub fn open_order_count(&self) -> Result<usize> {
        Ok(self.open_order_snapshots()?.len())
    }

    pub fn cex_net_position(&self, exchange: Exchange, venue_symbol: &str) -> Result<Decimal> {
        let orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;
        let net_position = orders
            .values()
            .filter(|stored| {
                stored.exchange == exchange
                    && stored.venue_symbol.as_deref() == Some(venue_symbol)
            })
            .fold(Decimal::ZERO, |acc, stored| {
                let filled_qty = match stored.response.filled_quantity {
                    VenueQuantity::CexBaseQty(qty) => qty.0,
                    VenueQuantity::PolyShares(_) => Decimal::ZERO,
                };
                let signed_qty = match stored.side {
                    OrderSide::Buy => filled_qty,
                    OrderSide::Sell => -filled_qty,
                };
                acc + signed_qty
            });
        Ok(net_position)
    }
}

#[async_trait]
impl OrderExecutor for DryRunExecutor {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let exchange_order_id = self.next_order_id();
        let (exchange, symbol, venue_symbol) = Self::order_identity(&request);

        // Use orderbook-based fill determination if available
        let (status, filled_quantity, average_price, rejection_reason) =
            self.determine_fill(&request);

        let response = OrderResponse {
            client_order_id: match &request {
                OrderRequest::Poly(req) => req.client_order_id.clone(),
                OrderRequest::Cex(req) => req.client_order_id.clone(),
            },
            exchange_order_id: exchange_order_id.clone(),
            status,
            filled_quantity,
            average_price,
            rejection_reason,
            timestamp_ms: self.next_timestamp_ms(),
        };

        let mut orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;
        orders.insert(
            exchange_order_id,
            StoredOrder {
                exchange,
                symbol,
                venue_symbol,
                side: Self::order_side(&request),
                response: response.clone(),
            },
        );

        Ok(response)
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        let mut orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;

        if let Some(stored) = orders.get_mut(order_id) {
            if stored.exchange == exchange && Self::is_cancellable(stored.response.status) {
                stored.response.status = OrderStatus::Cancelled;
            }
        }

        Ok(())
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        let mut orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;

        let mut cancelled = 0u32;
        for stored in orders.values_mut() {
            if stored.exchange == exchange
                && &stored.symbol == symbol
                && Self::is_cancellable(stored.response.status)
            {
                stored.response.status = OrderStatus::Cancelled;
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        let orders = self
            .orders
            .lock()
            .map_err(|_| CoreError::Channel("dry-run order store poisoned".to_owned()))?;
        let stored = orders
            .get(order_id)
            .ok_or_else(|| CoreError::Channel(format!("order not found: {}", order_id.0)))?;

        if stored.exchange != exchange {
            return Err(CoreError::Channel(format!(
                "order {} exchange mismatch",
                order_id.0
            )));
        }

        Ok(stored.response.clone())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use polyalpha_core::{
        CexOrderRequest, ClientOrderId, OrderRequest, OrderSide, OrderType, PolyOrderRequest,
        PolySizingInstruction, TokenSide, UsdNotional,
    };

    use super::*;

    fn sample_poly_limit_order() -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: ClientOrderId("poly-1".to_owned()),
            symbol: Symbol::new("btc-100k-mar-2026"),
            token_side: TokenSide::Yes,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            sizing: PolySizingInstruction::BuyBudgetCap {
                max_cost_usd: UsdNotional(Decimal::new(490, 2)),
                max_avg_price: Price(Decimal::new(49, 2)),
                max_shares: PolyShares(Decimal::new(10, 0)),
            },
            time_in_force: TimeInForce::Gtc,
            post_only: true,
        })
    }

    fn sample_cex_market_order() -> OrderRequest {
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId("cex-1".to_owned()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            venue_symbol: "BTCUSDT".to_owned(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            price: None,
            base_qty: CexBaseQty(Decimal::new(2, 1)),
            time_in_force: TimeInForce::Ioc,
            reduce_only: false,
        })
    }

    fn sample_zero_qty_cex_market_order() -> OrderRequest {
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId("cex-zero".to_owned()),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            venue_symbol: "BTCUSDT".to_owned(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            price: None,
            base_qty: CexBaseQty::ZERO,
            time_in_force: TimeInForce::Ioc,
            reduce_only: false,
        })
    }

    fn sample_poly_provider() -> Arc<crate::orderbook_provider::InMemoryOrderbookProvider> {
        let provider = Arc::new(crate::orderbook_provider::InMemoryOrderbookProvider::new());
        provider.update(OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-price-only"),
            instrument: InstrumentKind::PolyYes,
            bids: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(34, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(200, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(33, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(200, 0))),
                },
            ],
            asks: vec![
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(31, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(300, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(33, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(400, 0))),
                },
                polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(35, 2)),
                    quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(500, 0))),
                },
            ],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });
        provider
    }

    #[tokio::test]
    async fn dry_run_limit_orders_rest_as_open() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(sample_poly_limit_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Open);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::PolyShares(PolyShares::ZERO)
        );
    }

    #[tokio::test]
    async fn dry_run_market_orders_without_orderbook_are_rejected() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Rejected);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::CexBaseQty(CexBaseQty::default())
        );
        assert_eq!(
            response.rejection_reason.as_deref(),
            Some("no_orderbook_for_fill")
        );
    }

    #[tokio::test]
    async fn dry_run_zero_quantity_orders_are_rejected() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(sample_zero_qty_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Rejected);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::CexBaseQty(CexBaseQty::ZERO)
        );
        assert_eq!(response.rejection_reason.as_deref(), Some("zero_quantity"));
    }

    #[tokio::test]
    async fn dry_run_budget_cap_poly_order_without_orderbook_is_rejected() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(OrderRequest::Poly(PolyOrderRequest {
                client_order_id: ClientOrderId("poly-notional".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::Yes,
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                sizing: PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd: UsdNotional(Decimal::new(125, 0)),
                    max_avg_price: Price::ONE,
                    max_shares: PolyShares(Decimal::new(125, 0)),
                },
                time_in_force: TimeInForce::Fok,
                post_only: false,
            }))
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Rejected);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::PolyShares(PolyShares::ZERO)
        );
        assert_eq!(
            response.rejection_reason.as_deref(),
            Some("no_orderbook_for_fill")
        );
    }

    #[test]
    fn buy_budget_cap_stops_at_max_cost_usd() {
        let executor = DryRunExecutor::with_orderbook(
            sample_poly_provider(),
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        );
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
        let executor = DryRunExecutor::with_orderbook(
            sample_poly_provider(),
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        );
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
        assert_eq!(
            estimate.rejection_reason.as_deref(),
            Some("poly_min_proceeds_not_met")
        );
    }

    #[tokio::test]
    async fn dry_run_with_orderbook_and_custom_start_timestamp_uses_configured_clock() {
        let provider = Arc::new(crate::orderbook_provider::InMemoryOrderbookProvider::new());
        provider.update_cex(
            OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-100k-mar-2026"),
                instrument: InstrumentKind::CexPerp,
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1000, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1010, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                exchange_timestamp_ms: 1_700_000_000_000,
                received_at_ms: 1_700_000_000_000,
                sequence: 1,
                last_trade_price: None,
            },
            "BTCUSDT".to_owned(),
        );

        let executor = DryRunExecutor::with_orderbook_and_start_timestamp(
            provider,
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
            1_234_567_890,
        );

        let response = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.timestamp_ms, 1_234_567_890);
    }

    #[tokio::test]
    async fn dry_run_open_order_snapshots_only_include_open_like_status() {
        let executor = DryRunExecutor::new();
        let _open = executor
            .submit_order(sample_poly_limit_order())
            .await
            .expect("limit submit should succeed");
        let _filled = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("market submit should succeed");

        let open_orders = executor
            .open_order_snapshots()
            .expect("open snapshots should succeed");
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].status, OrderStatus::Open);
        assert_eq!(
            executor
                .open_order_count()
                .expect("open count should succeed"),
            1
        );
    }

    #[tokio::test]
    async fn dry_run_market_orders_fill_from_orderbook() {
        let provider = Arc::new(crate::orderbook_provider::InMemoryOrderbookProvider::new());
        provider.update_cex(
            OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-100k-mar-2026"),
                instrument: InstrumentKind::CexPerp,
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1000, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1010, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                exchange_timestamp_ms: 1_700_000_000_000,
                received_at_ms: 1_700_000_000_000,
                sequence: 1,
                last_trade_price: None,
            },
            "BTCUSDT".to_owned(),
        );
        let executor = DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        );

        let response = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Filled);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(2, 1)))
        );
        assert_eq!(response.average_price, Some(Price(Decimal::new(9998, 2))));
        assert_eq!(response.rejection_reason, None);
    }

    #[tokio::test]
    async fn dry_run_cex_market_orders_ignore_poly_liquidity_threshold() {
        let provider = Arc::new(crate::orderbook_provider::InMemoryOrderbookProvider::new());
        provider.update_cex(
            OrderBookSnapshot {
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-100k-mar-2026"),
                instrument: InstrumentKind::CexPerp,
                bids: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1000, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                asks: vec![polyalpha_core::PriceLevel {
                    price: Price(Decimal::new(1010, 1)),
                    quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(10, 0))),
                }],
                exchange_timestamp_ms: 1_700_000_000_000,
                received_at_ms: 1_700_000_000_000,
                sequence: 1,
                last_trade_price: None,
            },
            "BTCUSDT".to_owned(),
        );
        let executor = DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(100, 0),
                allow_partial_fill: false,
            },
        );

        let response = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Filled);
        assert_eq!(response.rejection_reason, None);
    }

    #[tokio::test]
    async fn dry_run_poly_slippage_is_clamped_to_probability_bounds() {
        let provider = Arc::new(crate::orderbook_provider::InMemoryOrderbookProvider::new());
        provider.update(OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: Symbol::new("btc-100k-mar-2026"),
            instrument: InstrumentKind::PolyNo,
            bids: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(998, 3)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(1000, 0))),
            }],
            asks: vec![polyalpha_core::PriceLevel {
                price: Price(Decimal::new(999, 3)),
                quantity: VenueQuantity::PolyShares(PolyShares(Decimal::new(1000, 0))),
            }],
            exchange_timestamp_ms: 1_700_000_000_000,
            received_at_ms: 1_700_000_000_000,
            sequence: 1,
            last_trade_price: None,
        });
        let executor = DryRunExecutor::with_orderbook(
            provider,
            SlippageConfig {
                poly_slippage_bps: 50,
                cex_slippage_bps: 2,
                min_liquidity: Decimal::new(1, 0),
                allow_partial_fill: false,
            },
        );

        let response = executor
            .submit_order(OrderRequest::Poly(PolyOrderRequest {
                client_order_id: ClientOrderId("poly-clamp".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::No,
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                sizing: PolySizingInstruction::BuyBudgetCap {
                    max_cost_usd: UsdNotional(Decimal::new(10, 0)),
                    max_avg_price: Price::ONE,
                    max_shares: PolyShares(Decimal::new(10, 0)),
                },
                time_in_force: TimeInForce::Fok,
                post_only: false,
            }))
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Filled);
        assert_eq!(response.average_price, Some(Price::ONE));
        assert_eq!(response.rejection_reason, None);
    }
}
