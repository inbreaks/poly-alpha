use async_trait::async_trait;

use polyalpha_core::{
    CoreError, Exchange, OrderExecutor, OrderId, OrderRequest, OrderResponse, Result, Symbol,
};

#[derive(Clone, Debug, Default)]
pub struct PolymarketExecutor;

impl PolymarketExecutor {
    pub fn new() -> Self {
        Self
    }

    fn unsupported() -> CoreError {
        CoreError::Channel(
            "polymarket live executor is not implemented yet; integrate official sdk sign+post endpoints here"
                .to_owned(),
        )
    }
}

#[async_trait]
impl OrderExecutor for PolymarketExecutor {
    async fn submit_order(&self, _request: OrderRequest) -> Result<OrderResponse> {
        Err(Self::unsupported())
    }

    async fn cancel_order(&self, _exchange: Exchange, _order_id: &OrderId) -> Result<()> {
        Err(Self::unsupported())
    }

    async fn cancel_all(&self, _exchange: Exchange, _symbol: &Symbol) -> Result<u32> {
        Err(Self::unsupported())
    }

    async fn query_order(&self, _exchange: Exchange, _order_id: &OrderId) -> Result<OrderResponse> {
        Err(Self::unsupported())
    }
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{OrderRequest, OrderSide};

    use super::*;

    #[tokio::test]
    async fn returns_explicit_not_implemented_error() {
        let executor = PolymarketExecutor::new();
        let err = executor
            .submit_order(OrderRequest::Cex(polyalpha_core::CexOrderRequest {
                client_order_id: polyalpha_core::ClientOrderId("cid".to_owned()),
                exchange: Exchange::Binance,
                symbol: Symbol::new("btc-100k-mar-2026"),
                venue_symbol: "BTCUSDT".to_owned(),
                side: OrderSide::Buy,
                order_type: polyalpha_core::OrderType::Market,
                price: None,
                base_qty: polyalpha_core::CexBaseQty::default(),
                time_in_force: polyalpha_core::TimeInForce::Ioc,
                reduce_only: false,
            }))
            .await
            .expect_err("must return not implemented");
        match err {
            CoreError::Channel(text) => {
                assert!(text.contains("not implemented"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
