use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionType {
    BasisHedge,
    DeltaRebalance,
    NegRiskArb,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LegStatus {
    Pending,
    Submitted,
    PartialFill,
    Filled,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HedgeState {
    Idle,
    SubmittingLegs,
    Hedging,
    Hedged,
    Rebalancing,
    Closing,
}
