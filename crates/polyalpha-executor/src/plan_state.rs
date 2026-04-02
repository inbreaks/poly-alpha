use polyalpha_core::{PlanningIntent, TradePlan};

#[derive(Clone, Debug)]
pub struct InFlightPlan {
    pub plan: TradePlan,
    pub state: PlanLifecycleState,
    pub idempotency_key: String,
}

impl InFlightPlan {
    pub fn new(plan: TradePlan) -> Self {
        Self {
            idempotency_key: plan.idempotency_key.clone(),
            plan,
            state: PlanLifecycleState::PlanReady,
        }
    }

    pub fn priority_rank(&self) -> u8 {
        priority_rank(&self.plan.priority)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanLifecycleState {
    PlanReady,
    SubmittingPoly,
    HedgingCex,
    VerifyingResidual,
    Recovering,
    Frozen,
}

pub fn priority_for_intent(intent: &PlanningIntent) -> &'static str {
    match intent {
        PlanningIntent::ForceExit { .. } => "force_exit",
        PlanningIntent::ResidualRecovery { .. } => "residual_recovery",
        PlanningIntent::ClosePosition { .. } => "close_position",
        PlanningIntent::DeltaRebalance { .. } => "delta_rebalance",
        PlanningIntent::OpenPosition { .. } => "open_position",
    }
}

pub fn priority_rank(priority: &str) -> u8 {
    match priority {
        "force_exit" => 5,
        "residual_recovery" => 4,
        "close_position" => 3,
        "delta_rebalance" => 2,
        "open_position" => 1,
        _ => 0,
    }
}
