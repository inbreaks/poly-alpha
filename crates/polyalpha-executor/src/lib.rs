pub mod binance;
pub mod dry_run;
pub mod manager;
pub mod okx;
pub mod orderbook_provider;
pub mod plan_state;
pub mod planner;
pub mod polymarket;
pub mod router;

pub use binance::{BinanceFuturesExecutor, BinanceSignedRequestPreview};
pub use dry_run::DryRunExecutor;
pub use manager::ExecutionManager;
pub use okx::{OkxExecutor, OkxSignedRequestPreview};
pub use orderbook_provider::{InMemoryOrderbookProvider, NoOpOrderbookProvider, OrderbookProvider};
pub use plan_state::{InFlightPlan, PlanLifecycleState};
pub use planner::{CanonicalPlanningContext, ExecutionPlanner, PlanRejection};
pub use polymarket::PolymarketExecutor;
pub use router::LiveExecutionRouter;

pub fn crate_status() -> &'static str {
    "polyalpha-executor live+dry-run ready"
}
