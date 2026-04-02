use std::sync::Arc;

use async_trait::async_trait;

use polyalpha_core::{
    CoreError, Exchange, OrderExecutor, OrderId, OrderRequest, OrderResponse, Result, Symbol,
};

use crate::{BinanceFuturesExecutor, OkxExecutor, PolymarketExecutor};

#[derive(Clone)]
pub struct LiveExecutionRouter {
    polymarket: Arc<dyn OrderExecutor>,
    cex_exchange: Exchange,
    cex: Arc<dyn OrderExecutor>,
}

impl std::fmt::Debug for LiveExecutionRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveExecutionRouter")
            .field("cex_exchange", &self.cex_exchange)
            .finish_non_exhaustive()
    }
}

impl LiveExecutionRouter {
    pub fn binance(polymarket: PolymarketExecutor, executor: BinanceFuturesExecutor) -> Self {
        Self {
            polymarket: Arc::new(polymarket),
            cex_exchange: Exchange::Binance,
            cex: Arc::new(executor),
        }
    }

    pub fn okx(polymarket: PolymarketExecutor, executor: OkxExecutor) -> Self {
        Self {
            polymarket: Arc::new(polymarket),
            cex_exchange: Exchange::Okx,
            cex: Arc::new(executor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_parts(
        polymarket: Arc<dyn OrderExecutor>,
        cex_exchange: Exchange,
        cex: Arc<dyn OrderExecutor>,
    ) -> Self {
        Self {
            polymarket,
            cex_exchange,
            cex,
        }
    }

    fn ensure_supported_cex(&self, exchange: Exchange) -> Result<()> {
        if exchange == self.cex_exchange {
            Ok(())
        } else {
            Err(CoreError::Channel(format!(
                "live router configured for {:?}, but received {:?} request",
                self.cex_exchange, exchange
            )))
        }
    }
}

#[async_trait]
impl OrderExecutor for LiveExecutionRouter {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        match &request {
            OrderRequest::Poly(_) => self.polymarket.submit_order(request).await,
            OrderRequest::Cex(req) => {
                self.ensure_supported_cex(req.exchange)?;
                self.cex.submit_order(request).await
            }
        }
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        match exchange {
            Exchange::Polymarket => self.polymarket.cancel_order(exchange, order_id).await,
            exchange => {
                self.ensure_supported_cex(exchange)?;
                self.cex.cancel_order(exchange, order_id).await
            }
        }
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        match exchange {
            Exchange::Polymarket => self.polymarket.cancel_all(exchange, symbol).await,
            exchange => {
                self.ensure_supported_cex(exchange)?;
                self.cex.cancel_all(exchange, symbol).await
            }
        }
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        match exchange {
            Exchange::Polymarket => self.polymarket.query_order(exchange, order_id).await,
            exchange => {
                self.ensure_supported_cex(exchange)?;
                self.cex.query_order(exchange, order_id).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rust_decimal::Decimal;

    use polyalpha_core::{
        CexBaseQty, CexOrderRequest, ClientOrderId, OrderSide, OrderStatus, OrderType,
        PolyOrderRequest, PolyShares, PolySizingInstruction, Price, TimeInForce, TokenSide,
        UsdNotional, VenueQuantity,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingExecutor {
        submits: Arc<Mutex<u32>>,
        cancel_orders: Arc<Mutex<u32>>,
        cancel_alls: Arc<Mutex<u32>>,
        queries: Arc<Mutex<u32>>,
    }

    impl RecordingExecutor {
        fn submit_count(&self) -> u32 {
            *self.submits.lock().expect("submit count lock")
        }

        fn cancel_order_count(&self) -> u32 {
            *self.cancel_orders.lock().expect("cancel order count lock")
        }

        fn cancel_all_count(&self) -> u32 {
            *self.cancel_alls.lock().expect("cancel all count lock")
        }

        fn query_count(&self) -> u32 {
            *self.queries.lock().expect("query count lock")
        }
    }

    #[async_trait]
    impl OrderExecutor for RecordingExecutor {
        async fn submit_order(&self, _request: OrderRequest) -> Result<OrderResponse> {
            *self.submits.lock().expect("submit lock") += 1;
            Ok(OrderResponse {
                client_order_id: ClientOrderId("client-1".to_owned()),
                exchange_order_id: OrderId("order-1".to_owned()),
                status: OrderStatus::Filled,
                filled_quantity: VenueQuantity::PolyShares(PolyShares(Decimal::ONE)),
                average_price: Some(Price(Decimal::new(50, 2))),
                rejection_reason: None,
                timestamp_ms: 1,
            })
        }

        async fn cancel_order(&self, _exchange: Exchange, _order_id: &OrderId) -> Result<()> {
            *self.cancel_orders.lock().expect("cancel order lock") += 1;
            Ok(())
        }

        async fn cancel_all(&self, _exchange: Exchange, _symbol: &Symbol) -> Result<u32> {
            *self.cancel_alls.lock().expect("cancel all lock") += 1;
            Ok(1)
        }

        async fn query_order(
            &self,
            _exchange: Exchange,
            _order_id: &OrderId,
        ) -> Result<OrderResponse> {
            *self.queries.lock().expect("query lock") += 1;
            Ok(OrderResponse {
                client_order_id: ClientOrderId("client-1".to_owned()),
                exchange_order_id: OrderId("order-1".to_owned()),
                status: OrderStatus::Filled,
                filled_quantity: VenueQuantity::CexBaseQty(CexBaseQty(Decimal::ONE)),
                average_price: Some(Price(Decimal::new(100_000, 0))),
                rejection_reason: None,
                timestamp_ms: 2,
            })
        }
    }

    fn sample_symbol() -> Symbol {
        Symbol::new("btc-live-router")
    }

    fn sample_poly_request() -> OrderRequest {
        OrderRequest::Poly(PolyOrderRequest {
            client_order_id: ClientOrderId("poly-client".to_owned()),
            symbol: sample_symbol(),
            token_side: TokenSide::Yes,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            sizing: PolySizingInstruction::BuyBudgetCap {
                max_cost_usd: UsdNotional(Decimal::new(200, 0)),
                max_avg_price: Price(Decimal::new(50, 2)),
                max_shares: PolyShares(Decimal::new(400, 0)),
            },
            time_in_force: TimeInForce::Ioc,
            post_only: false,
        })
    }

    fn sample_cex_request(exchange: Exchange) -> OrderRequest {
        OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId("cex-client".to_owned()),
            exchange,
            symbol: sample_symbol(),
            venue_symbol: "BTCUSDT".to_owned(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            price: None,
            base_qty: CexBaseQty(Decimal::new(5, 3)),
            time_in_force: TimeInForce::Ioc,
            reduce_only: true,
        })
    }

    #[tokio::test]
    async fn submit_order_routes_poly_requests_to_polymarket_executor() {
        let poly = RecordingExecutor::default();
        let cex = RecordingExecutor::default();
        let router = LiveExecutionRouter::with_parts(
            Arc::new(poly.clone()),
            Exchange::Binance,
            Arc::new(cex.clone()),
        );

        router.submit_order(sample_poly_request()).await.unwrap();

        assert_eq!(poly.submit_count(), 1);
        assert_eq!(cex.submit_count(), 0);
    }

    #[tokio::test]
    async fn submit_order_routes_cex_requests_to_selected_venue_executor() {
        let poly = RecordingExecutor::default();
        let cex = RecordingExecutor::default();
        let router = LiveExecutionRouter::with_parts(
            Arc::new(poly.clone()),
            Exchange::Okx,
            Arc::new(cex.clone()),
        );

        router
            .submit_order(sample_cex_request(Exchange::Okx))
            .await
            .unwrap();

        assert_eq!(poly.submit_count(), 0);
        assert_eq!(cex.submit_count(), 1);
    }

    #[tokio::test]
    async fn cancel_and_query_route_by_exchange() {
        let poly = RecordingExecutor::default();
        let cex = RecordingExecutor::default();
        let router = LiveExecutionRouter::with_parts(
            Arc::new(poly.clone()),
            Exchange::Binance,
            Arc::new(cex.clone()),
        );
        let order_id = OrderId("order-1".to_owned());

        router
            .cancel_order(Exchange::Polymarket, &order_id)
            .await
            .unwrap();
        router
            .cancel_all(Exchange::Binance, &sample_symbol())
            .await
            .unwrap();
        router
            .query_order(Exchange::Binance, &order_id)
            .await
            .unwrap();

        assert_eq!(poly.cancel_order_count(), 1);
        assert_eq!(cex.cancel_all_count(), 1);
        assert_eq!(cex.query_count(), 1);
    }

    #[tokio::test]
    async fn mismatched_cex_request_is_rejected() {
        let router = LiveExecutionRouter::with_parts(
            Arc::new(RecordingExecutor::default()),
            Exchange::Binance,
            Arc::new(RecordingExecutor::default()),
        );

        let err = router
            .submit_order(sample_cex_request(Exchange::Okx))
            .await
            .expect_err("mismatched cex exchange should fail");

        assert!(err.to_string().contains("configured for Binance"));
    }
}
