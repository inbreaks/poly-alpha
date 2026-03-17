use async_trait::async_trait;

use crate::event::ConnectionStatus;
use crate::types::{Exchange, Symbol};
use crate::Result;

#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn subscribe_market(&self, symbol: &Symbol) -> Result<()>;
    fn connection_status(&self) -> ConnectionStatus;
    fn exchange(&self) -> Exchange;
}
