use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use polyalpha_core::{
    CexBaseQty, CoreError, Exchange, InstrumentKind, OrderBookSnapshot, OrderExecutor, OrderId,
    OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PolyShares, Price, Result,
    Symbol, TimeInForce, VenueQuantity,
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

    /// Determine order status and fill details based on orderbook.
    fn determine_fill(
        &self,
        request: &OrderRequest,
    ) -> (OrderStatus, VenueQuantity, Option<Price>, Option<String>) {
        let requested_qty = Self::requested_quantity(request);
        if Self::extract_qty(&requested_qty) <= Decimal::ZERO {
            return (
                OrderStatus::Rejected,
                Self::zero_quantity_for_request(request),
                None,
                Some("zero_quantity".to_owned()),
            );
        }

        if self.orderbook_provider.is_none() {
            return if Self::is_gtc_limit(request) {
                (
                    OrderStatus::Open,
                    Self::zero_quantity_for_request(request),
                    Self::initial_price(request),
                    None,
                )
            } else {
                (
                    OrderStatus::Rejected,
                    Self::zero_quantity_for_request(request),
                    None,
                    Some("no_orderbook_for_fill".to_owned()),
                )
            };
        }

        // For limit orders with GTC, keep as Open
        if Self::is_gtc_limit(request) {
            return (
                OrderStatus::Open,
                Self::zero_quantity_for_request(request),
                Self::initial_price(request),
                None,
            );
        }

        // Try to get orderbook
        let Some(orderbook) = self.get_orderbook_for_request(request) else {
            return (
                OrderStatus::Rejected,
                Self::zero_quantity_for_request(request),
                None,
                Some("no_orderbook_for_fill".to_owned()),
            );
        };

        // Get side and quantity
        let (side, is_poly) = match request {
            OrderRequest::Poly(req) => (req.side, true),
            OrderRequest::Cex(req) => (req.side, false),
        };
        let requested_qty = Self::extract_qty(&requested_qty);

        // Check minimum liquidity for Polymarket only. CEX hedge sizes are small
        // base quantities, so a global "100" threshold would incorrectly reject
        // otherwise fillable hedge legs.
        let available_liquidity: Decimal = {
            let levels = match side {
                OrderSide::Buy => &orderbook.asks,
                OrderSide::Sell => &orderbook.bids,
            };
            levels.iter().map(|l| Self::extract_qty(&l.quantity)).sum()
        };

        if is_poly && available_liquidity < self.slippage_config.min_liquidity {
            // Insufficient liquidity, reject
            return (
                OrderStatus::Rejected,
                Self::zero_quantity_for_request(request),
                None,
                Some("insufficient_liquidity".to_owned()),
            );
        }

        // Calculate fill
        let fill = self.calculate_fill_price(&orderbook, side, requested_qty);

        if !fill.is_complete && !self.slippage_config.allow_partial_fill {
            // Partial fill not allowed, reject
            return (
                OrderStatus::Rejected,
                Self::zero_quantity_for_request(request),
                None,
                Some("partial_fill_not_allowed".to_owned()),
            );
        }

        // Apply slippage
        let final_price = self.apply_slippage(fill.average_price, side, is_poly);

        let filled_qty = if fill.is_complete {
            Self::requested_quantity(request)
        } else {
            // Partial fill
            match request {
                OrderRequest::Poly(_) => {
                    VenueQuantity::PolyShares(PolyShares(fill.filled_quantity))
                }
                OrderRequest::Cex(_) => VenueQuantity::CexBaseQty(CexBaseQty(fill.filled_quantity)),
            }
        };

        let status = if fill.is_complete {
            OrderStatus::Filled
        } else {
            OrderStatus::PartialFill
        };

        (status, filled_qty, Some(final_price), None)
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
            OrderRequest::Poly(req) => {
                if let Some(shares) = req.shares {
                    VenueQuantity::PolyShares(shares)
                } else if let Some(notional) = req.quote_notional {
                    // 边界约束: 仅给 dry-run 使用，notional-only 单按 $1/share 映射。
                    VenueQuantity::PolyShares(PolyShares(notional.0))
                } else {
                    VenueQuantity::PolyShares(PolyShares::ZERO)
                }
            }
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

    fn initial_price(request: &OrderRequest) -> Option<Price> {
        match request {
            OrderRequest::Poly(req) => req.limit_price.or(Some(Price::ONE)),
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
        TokenSide, UsdNotional,
    };

    use super::*;

    fn sample_poly_limit_order() -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: ClientOrderId("poly-1".to_owned()),
            symbol: Symbol::new("btc-100k-mar-2026"),
            token_side: TokenSide::Yes,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Price(Decimal::new(49, 2))),
            shares: Some(PolyShares(Decimal::new(10, 0))),
            quote_notional: None,
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
    async fn dry_run_notional_only_poly_order_maps_to_shares() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(OrderRequest::Poly(PolyOrderRequest {
                client_order_id: ClientOrderId("poly-notional".to_owned()),
                symbol: Symbol::new("btc-100k-mar-2026"),
                token_side: TokenSide::Yes,
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                limit_price: None,
                shares: None,
                quote_notional: Some(UsdNotional(Decimal::new(125, 0))),
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
                limit_price: None,
                shares: Some(PolyShares(Decimal::new(10, 0))),
                quote_notional: None,
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
