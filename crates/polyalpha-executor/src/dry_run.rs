use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use polyalpha_core::{
    CexBaseQty, CoreError, Exchange, OrderExecutor, OrderId, OrderRequest, OrderResponse,
    OrderStatus, OrderType, PolyShares, Price, Result, Symbol, TimeInForce, VenueQuantity,
};

const DEFAULT_START_TS_MS: u64 = 1_700_000_000_000;

#[derive(Clone, Debug)]
pub struct DryRunExecutor {
    orders: Arc<Mutex<HashMap<OrderId, StoredOrder>>>,
    next_order_seq: Arc<AtomicU64>,
    next_timestamp_ms: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct StoredOrder {
    exchange: Exchange,
    symbol: Symbol,
    response: OrderResponse,
}

impl Default for DryRunExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DryRunExecutor {
    pub fn new() -> Self {
        Self::with_start_timestamp(DEFAULT_START_TS_MS)
    }

    pub fn with_start_timestamp(start_timestamp_ms: u64) -> Self {
        Self {
            orders: Arc::new(Mutex::new(HashMap::new())),
            next_order_seq: Arc::new(AtomicU64::new(1)),
            next_timestamp_ms: Arc::new(AtomicU64::new(start_timestamp_ms)),
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

    fn exchange_and_symbol(request: &OrderRequest) -> (Exchange, Symbol) {
        match request {
            OrderRequest::Poly(req) => (Exchange::Polymarket, req.symbol.clone()),
            OrderRequest::Cex(req) => (req.exchange, req.symbol.clone()),
        }
    }

    fn initial_status(request: &OrderRequest) -> OrderStatus {
        match request {
            OrderRequest::Poly(req) => {
                if req.post_only
                    || (matches!(req.order_type, OrderType::Limit | OrderType::PostOnly)
                        && matches!(req.time_in_force, TimeInForce::Gtc))
                {
                    OrderStatus::Open
                } else {
                    OrderStatus::Filled
                }
            }
            OrderRequest::Cex(req) => {
                if matches!(req.order_type, OrderType::Limit | OrderType::PostOnly)
                    && matches!(req.time_in_force, TimeInForce::Gtc)
                {
                    OrderStatus::Open
                } else {
                    OrderStatus::Filled
                }
            }
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
}

#[async_trait]
impl OrderExecutor for DryRunExecutor {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        let exchange_order_id = self.next_order_id();
        let (exchange, symbol) = Self::exchange_and_symbol(&request);
        let status = Self::initial_status(&request);
        let filled_quantity = if matches!(status, OrderStatus::Filled) {
            Self::requested_quantity(&request)
        } else {
            Self::zero_quantity_for_request(&request)
        };
        let response = OrderResponse {
            client_order_id: match &request {
                OrderRequest::Poly(req) => req.client_order_id.clone(),
                OrderRequest::Cex(req) => req.client_order_id.clone(),
            },
            exchange_order_id: exchange_order_id.clone(),
            status,
            filled_quantity,
            average_price: Self::initial_price(&request),
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
        let stored = orders.get(order_id).ok_or_else(|| CoreError::Channel(format!(
            "order not found: {}",
            order_id.0
        )))?;

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
    async fn dry_run_market_orders_fill_immediately() {
        let executor = DryRunExecutor::new();
        let response = executor
            .submit_order(sample_cex_market_order())
            .await
            .expect("submit should succeed");

        assert_eq!(response.status, OrderStatus::Filled);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::CexBaseQty(CexBaseQty(Decimal::new(2, 1)))
        );
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

        assert_eq!(response.status, OrderStatus::Filled);
        assert_eq!(
            response.filled_quantity,
            VenueQuantity::PolyShares(PolyShares(Decimal::new(125, 0)))
        );
    }
}
