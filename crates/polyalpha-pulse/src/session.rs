use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::model::{GammaCapMode, PulseAsset, PulseFailureCode, PulseSessionState, SessionCommand};

#[derive(Clone, Debug, PartialEq)]
pub struct PulseSessionConfig {
    pub session_id: String,
    pub asset: PulseAsset,
    pub planned_poly_qty: Decimal,
    pub min_opening_notional_usd: Decimal,
    pub gamma_cap_mode: GammaCapMode,
    pub max_abs_event_delta: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PolyFillOutcome {
    pub planned_qty: Decimal,
    pub filled_qty: Decimal,
    pub avg_price: Decimal,
}

#[derive(Clone, Debug)]
pub struct PulseSession {
    config: PulseSessionConfig,
    state: PulseSessionState,
    actual_poly_filled_qty: Decimal,
    target_event_delta: f64,
    target_delta_exposure: Decimal,
    pin_risk_active: bool,
    pending_commands: VecDeque<SessionCommand>,
}

impl PulseSession {
    pub fn new(config: PulseSessionConfig) -> Self {
        Self {
            config,
            state: PulseSessionState::PolyOpening,
            actual_poly_filled_qty: Decimal::ZERO,
            target_event_delta: 0.0,
            target_delta_exposure: Decimal::ZERO,
            pin_risk_active: false,
            pending_commands: VecDeque::new(),
        }
    }

    pub fn on_poly_fill(&mut self, fill: PolyFillOutcome) {
        self.actual_poly_filled_qty = fill.filled_qty;
        let filled_notional = fill.filled_qty * fill.avg_price;
        if filled_notional < self.config.min_opening_notional_usd {
            self.actual_poly_filled_qty = Decimal::ZERO;
            self.target_event_delta = 0.0;
            self.target_delta_exposure = Decimal::ZERO;
            self.state = PulseSessionState::Closed;
            return;
        }

        self.state = PulseSessionState::HedgeOpening;
        self.pending_commands
            .push_back(SessionCommand::RequestHedgeReconcile {
                hedge_reference_qty: self.actual_poly_filled_qty,
            });
    }

    pub fn recompute_target_delta(&mut self, proposed_event_delta: f64) {
        let target_event_delta = if self.pin_risk_active {
            match self.config.gamma_cap_mode {
                GammaCapMode::DeltaClamp => proposed_event_delta.clamp(
                    -self.config.max_abs_event_delta,
                    self.config.max_abs_event_delta,
                ),
                GammaCapMode::ProtectiveOnly | GammaCapMode::Freeze => proposed_event_delta,
            }
        } else {
            proposed_event_delta
        };

        self.target_event_delta = target_event_delta;
        let abs_delta = Decimal::from_f64_retain(target_event_delta.abs()).unwrap_or(Decimal::ZERO);
        self.target_delta_exposure = self.actual_poly_filled_qty * abs_delta;
    }

    pub fn trigger_emergency_flatten(&mut self, reason: PulseFailureCode) {
        self.target_event_delta = 0.0;
        self.target_delta_exposure = Decimal::ZERO;
        self.state = PulseSessionState::Closed;
        self.pending_commands
            .push_back(SessionCommand::EmergencyFlatten { reason });
    }

    pub fn next_command(&mut self) -> Option<SessionCommand> {
        self.pending_commands.pop_front()
    }

    pub fn actual_poly_filled_qty(&self) -> Decimal {
        self.actual_poly_filled_qty
    }

    pub fn pin_risk_mode(&self) -> Option<GammaCapMode> {
        self.pin_risk_active.then_some(self.config.gamma_cap_mode)
    }

    pub fn target_event_delta(&self) -> f64 {
        self.target_event_delta
    }

    pub fn target_delta_exposure(&self) -> Decimal {
        self.target_delta_exposure
    }

    pub fn state(&self) -> PulseSessionState {
        self.state
    }

    #[cfg(test)]
    fn force_pin_risk_zone_for_test(&mut self, value: bool) {
        self.pin_risk_active = value;
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn test_session() -> PulseSession {
        PulseSession::new(PulseSessionConfig {
            session_id: "session-a".to_owned(),
            asset: PulseAsset::Btc,
            planned_poly_qty: Decimal::new(10_000, 0),
            min_opening_notional_usd: Decimal::new(250, 0),
            gamma_cap_mode: GammaCapMode::DeltaClamp,
            max_abs_event_delta: 0.75,
        })
    }

    fn test_session_in_pin_risk_zone() -> PulseSession {
        let mut session = test_session();
        session.force_pin_risk_zone_for_test(true);
        session
    }

    #[test]
    fn poly_opening_uses_actual_filled_qty_for_hedge_opening() {
        let mut session = test_session();
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });

        let cmd = session.next_command().expect("hedge opening command");

        assert_eq!(session.actual_poly_filled_qty(), Decimal::new(3_500, 0));
        assert_eq!(cmd.hedge_reference_qty(), Decimal::new(3_500, 0));
    }

    #[test]
    fn pin_risk_delta_clamp_caps_target_delta_before_hedge_storm() {
        let mut session = test_session_in_pin_risk_zone();
        session.recompute_target_delta(1.84);

        assert_eq!(session.pin_risk_mode(), Some(GammaCapMode::DeltaClamp));
        assert_eq!(session.target_event_delta(), 0.75);
    }

    #[test]
    fn emergency_flatten_zeroes_session_target_before_aggregator_release() {
        let mut session = test_session();
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });
        session.recompute_target_delta(0.42);
        session.trigger_emergency_flatten(PulseFailureCode::DataFreshnessRejected);

        assert_eq!(session.target_delta_exposure(), Decimal::ZERO);
        assert_eq!(session.state(), PulseSessionState::Closed);
    }
}
