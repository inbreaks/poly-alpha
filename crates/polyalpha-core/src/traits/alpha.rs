use async_trait::async_trait;

use crate::event::MarketDataEvent;
use crate::types::{AlphaEngineOutput, EngineParams};

#[async_trait]
pub trait AlphaEngine: Send + Sync {
    async fn on_market_data(&mut self, event: &MarketDataEvent) -> AlphaEngineOutput;
    fn update_params(&mut self, params: EngineParams);
}
