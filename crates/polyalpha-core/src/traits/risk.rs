use async_trait::async_trait;

use crate::types::{
    CircuitBreakerStatus, Fill, OrderRequest, RiskRejection, TradePlan, UsdNotional,
};
use crate::Result;

#[async_trait]
pub trait RiskManager: Send + Sync {
    async fn pre_trade_check(
        &self,
        request: OrderRequest,
    ) -> std::result::Result<OrderRequest, RiskRejection>;
    fn pre_trade_check_open_plan(&self, plan: &TradePlan)
        -> std::result::Result<(), RiskRejection>;
    async fn on_fill(&mut self, fill: &Fill) -> Result<()>;
    async fn apply_realized_pnl_adjustment(&mut self, adjustment: UsdNotional) -> Result<()>;
    fn circuit_breaker_status(&self) -> CircuitBreakerStatus;
    fn trigger_circuit_breaker(&mut self, reason: &str);
}
