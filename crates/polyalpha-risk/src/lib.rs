mod in_memory;
mod position_tracker;

pub use in_memory::{InMemoryRiskManager, RiskLimits};
pub use position_tracker::PositionTracker;

pub fn crate_status() -> &'static str {
    "polyalpha-risk in-memory mvp"
}
