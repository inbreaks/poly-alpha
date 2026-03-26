mod in_memory;

pub use in_memory::{InMemoryRiskManager, RiskLimits};
pub use polyalpha_core::PositionTracker;

pub fn crate_status() -> &'static str {
    "polyalpha-risk in-memory mvp"
}
