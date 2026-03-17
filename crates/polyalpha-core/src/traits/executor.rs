use async_trait::async_trait;

use crate::types::{Exchange, OrderId, OrderRequest, OrderResponse, Symbol};
use crate::Result;

#[async_trait]
pub trait OrderExecutor: Send + Sync {
    async fn submit_order(&self, request: OrderRequest) -> Result<OrderResponse>;
    async fn cancel_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<()>;
    async fn cancel_all(&self, exchange: Exchange, symbol: &Symbol) -> Result<u32>;
    async fn query_order(&self, exchange: Exchange, order_id: &OrderId) -> Result<OrderResponse>;
}
