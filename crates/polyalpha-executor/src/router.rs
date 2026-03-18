use async_trait::async_trait;

use polyalpha_core::{
    Exchange, OrderExecutor, OrderId, OrderRequest, OrderResponse, Result, Symbol,
};

use crate::{BinanceFuturesExecutor, OkxExecutor, PolymarketExecutor};

#[derive(Clone, Debug)]
pub enum LiveExecutionRouter {
    Binance(BinanceFuturesExecutor),
    Okx(OkxExecutor),
    Polymarket(PolymarketExecutor),
}

impl LiveExecutionRouter {
    pub fn binance(executor: BinanceFuturesExecutor) -> Self {
        Self::Binance(executor)
    }

    pub fn okx(executor: OkxExecutor) -> Self {
        Self::Okx(executor)
    }

    pub fn polymarket(executor: PolymarketExecutor) -> Self {
        Self::Polymarket(executor)
    }
}

#[async_trait]
impl OrderExecutor for LiveExecutionRouter {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse> {
        match self {
            Self::Binance(inner) => inner.submit_order(request).await,
            Self::Okx(inner) => inner.submit_order(request).await,
            Self::Polymarket(inner) => inner.submit_order(request).await,
        }
    }

    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()> {
        match self {
            Self::Binance(inner) => inner.cancel_order(exchange, order_id).await,
            Self::Okx(inner) => inner.cancel_order(exchange, order_id).await,
            Self::Polymarket(inner) => inner.cancel_order(exchange, order_id).await,
        }
    }

    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32> {
        match self {
            Self::Binance(inner) => inner.cancel_all(exchange, symbol).await,
            Self::Okx(inner) => inner.cancel_all(exchange, symbol).await,
            Self::Polymarket(inner) => inner.cancel_all(exchange, symbol).await,
        }
    }

    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse> {
        match self {
            Self::Binance(inner) => inner.query_order(exchange, order_id).await,
            Self::Okx(inner) => inner.query_order(exchange, order_id).await,
            Self::Polymarket(inner) => inner.query_order(exchange, order_id).await,
        }
    }
}
