use async_trait::async_trait;

use crate::types::{CircuitBreakerStatus, Fill, OrderRequest, RiskRejection};
use crate::Result;

#[async_trait]
pub trait RiskManager: Send + Sync {
    async fn pre_trade_check(
        &self,
        request: OrderRequest,
    ) -> std::result::Result<OrderRequest, RiskRejection>;
    async fn on_fill(&mut self, fill: &Fill) -> Result<()>;
    fn circuit_breaker_status(&self) -> CircuitBreakerStatus;
    fn trigger_circuit_breaker(&mut self, reason: &str);
}
