use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CircuitBreakerStatus {
    #[default]
    Closed,
    HalfOpen,
    Open,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum RiskRejection {
    #[error("circuit breaker is open")]
    CircuitBreakerOpen,
    #[error("market is not tradable in the current phase")]
    MarketPhaseBlocked,
    #[error("persistence lag exceeded threshold")]
    PersistenceDegraded,
    #[error("quantity became zero after precision normalization")]
    PrecisionRoundedToZero,
    #[error("risk limit breached: {0}")]
    LimitBreached(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Error)]
#[error("persistence lag {lag_secs}s exceeded max {max_lag_secs}s")]
pub struct PersistenceLag {
    pub lag_secs: u64,
    pub max_lag_secs: u64,
}
