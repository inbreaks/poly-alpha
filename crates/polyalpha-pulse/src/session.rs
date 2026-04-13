use std::collections::VecDeque;

use polyalpha_core::TokenSide;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::model::{GammaCapMode, PulseAsset, PulseFailureCode, PulseSessionState, SessionCommand};

#[derive(Clone, Debug, PartialEq)]
pub struct PulseSessionConfig {
    pub session_id: String,
    pub asset: PulseAsset,
    pub claim_side: TokenSide,
    pub planned_poly_qty: Decimal,
    pub min_opening_notional_usd: Decimal,
    pub min_open_fill_ratio: Decimal,
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
        let actual_fill_ratio = if fill.planned_qty <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            fill.filled_qty / fill.planned_qty
        };
        if filled_notional < self.config.min_opening_notional_usd
            || actual_fill_ratio < self.config.min_open_fill_ratio
        {
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
                GammaCapMode::ProtectiveOnly => {
                    if self.target_event_delta == 0.0
                        || proposed_event_delta.abs() <= self.target_event_delta.abs()
                    {
                        proposed_event_delta
                    } else {
                        self.target_event_delta
                    }
                }
                GammaCapMode::Freeze => {
                    if self.target_event_delta == 0.0 {
                        proposed_event_delta
                    } else {
                        self.target_event_delta
                    }
                }
            }
        } else {
            proposed_event_delta
        };

        self.target_event_delta = target_event_delta;
        let signed_hedge_delta = match self.config.claim_side {
            TokenSide::Yes => -target_event_delta,
            TokenSide::No => target_event_delta,
        };
        let signed_delta =
            Decimal::from_f64_retain(signed_hedge_delta).unwrap_or(Decimal::ZERO);
        self.target_delta_exposure = self.actual_poly_filled_qty * signed_delta;
    }

    pub fn trigger_emergency_flatten(&mut self, reason: PulseFailureCode) {
        self.target_event_delta = 0.0;
        self.target_delta_exposure = Decimal::ZERO;
        self.state = PulseSessionState::EmergencyFlatten;
        self.pending_commands
            .push_back(SessionCommand::EmergencyFlatten { reason });
    }

    pub fn next_command(&mut self) -> Option<SessionCommand> {
        self.pending_commands.pop_front()
    }

    pub fn mark_hedge_opened(&mut self) {
        if self.state == PulseSessionState::HedgeOpening {
            self.state = PulseSessionState::MakerExitWorking;
        }
    }

    pub fn mark_maker_exit_working(&mut self) {
        self.state = PulseSessionState::MakerExitWorking;
    }

    pub fn mark_pegging(&mut self) {
        self.state = PulseSessionState::Pegging;
    }

    pub fn mark_rehedging(&mut self) {
        self.state = PulseSessionState::Rehedging;
    }

    pub fn start_chasing_exit(&mut self) {
        self.state = PulseSessionState::ChasingExit;
    }

    pub fn mark_closed(&mut self) {
        self.state = PulseSessionState::Closed;
    }

    pub fn actual_poly_filled_qty(&self) -> Decimal {
        self.actual_poly_filled_qty
    }

    pub fn actual_poly_fill_ratio(&self) -> f64 {
        if self.config.planned_poly_qty.is_zero() {
            return 0.0;
        }

        (self.actual_poly_filled_qty / self.config.planned_poly_qty)
            .to_f64()
            .unwrap_or_default()
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

    pub fn session_id(&self) -> &str {
        &self.config.session_id
    }

    pub fn asset(&self) -> PulseAsset {
        self.config.asset
    }

    pub fn planned_poly_qty(&self) -> Decimal {
        self.config.planned_poly_qty
    }

    pub fn claim_side(&self) -> TokenSide {
        self.config.claim_side
    }

    pub fn set_pin_risk_active(&mut self, value: bool) {
        self.pin_risk_active = value;
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
            claim_side: TokenSide::No,
            planned_poly_qty: Decimal::new(10_000, 0),
            min_opening_notional_usd: Decimal::new(250, 0),
            min_open_fill_ratio: Decimal::ZERO,
            gamma_cap_mode: GammaCapMode::DeltaClamp,
            max_abs_event_delta: 0.75,
        })
    }

    fn test_session_in_pin_risk_zone() -> PulseSession {
        let mut session = test_session();
        session.force_pin_risk_zone_for_test(true);
        session
    }

    fn test_session_with_gamma_mode(gamma_cap_mode: GammaCapMode) -> PulseSession {
        let mut session = PulseSession::new(PulseSessionConfig {
            session_id: "session-gamma".to_owned(),
            asset: PulseAsset::Btc,
            claim_side: TokenSide::No,
            planned_poly_qty: Decimal::new(10_000, 0),
            min_opening_notional_usd: Decimal::new(250, 0),
            min_open_fill_ratio: Decimal::ZERO,
            gamma_cap_mode,
            max_abs_event_delta: 0.75,
        });
        session.force_pin_risk_zone_for_test(true);
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });
        session
    }

    fn test_session_with_opening_guards(
        min_opening_notional_usd: Decimal,
        min_open_fill_ratio: Decimal,
    ) -> PulseSession {
        PulseSession::new(PulseSessionConfig {
            session_id: "session-opening-guards".to_owned(),
            asset: PulseAsset::Btc,
            claim_side: TokenSide::No,
            planned_poly_qty: Decimal::new(10_000, 0),
            min_opening_notional_usd,
            min_open_fill_ratio,
            gamma_cap_mode: GammaCapMode::DeltaClamp,
            max_abs_event_delta: 0.75,
        })
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
    fn no_claim_maps_to_positive_hedge_target_delta_exposure() {
        let mut session = test_session();
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });
        session.recompute_target_delta(Decimal::new(4, 4).to_f64().unwrap());

        assert_eq!(session.claim_side(), TokenSide::No);
        assert_eq!(session.target_delta_exposure().round_dp(1), Decimal::new(14, 1));
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
        assert_eq!(session.state(), PulseSessionState::EmergencyFlatten);
    }

    #[test]
    fn timeout_exit_transitions_session_through_chasing_to_closed() {
        let mut session = test_session();
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(3_500, 0),
            avg_price: Decimal::new(35, 2),
        });

        session.mark_hedge_opened();
        session.start_chasing_exit();
        session.mark_closed();

        assert_eq!(session.state(), PulseSessionState::Closed);
    }

    #[test]
    fn below_min_poly_open_still_retains_actual_fill_qty_for_audit() {
        let mut session = test_session();
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(100, 0),
            avg_price: Decimal::new(35, 2),
        });

        assert_eq!(session.state(), PulseSessionState::Closed);
        assert_eq!(session.actual_poly_filled_qty(), Decimal::new(100, 0));
        assert_eq!(session.target_delta_exposure(), Decimal::ZERO);
    }

    #[test]
    fn partial_fill_below_min_fill_ratio_closes_session_even_if_notional_meets_threshold() {
        let mut session =
            test_session_with_opening_guards(Decimal::new(50, 0), Decimal::new(10, 2));
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(500, 0),
            avg_price: Decimal::new(35, 2),
        });

        assert_eq!(session.state(), PulseSessionState::Closed);
        assert_eq!(session.actual_poly_filled_qty(), Decimal::new(500, 0));
        assert_eq!(session.actual_poly_fill_ratio(), 0.05);
        assert_eq!(session.target_delta_exposure(), Decimal::ZERO);
    }

    #[test]
    fn partial_fill_above_min_fill_ratio_and_min_notional_enters_hedge_opening() {
        let mut session =
            test_session_with_opening_guards(Decimal::new(50, 0), Decimal::new(10, 2));
        session.on_poly_fill(PolyFillOutcome {
            planned_qty: Decimal::new(10_000, 0),
            filled_qty: Decimal::new(1_500, 0),
            avg_price: Decimal::new(10, 2),
        });

        let cmd = session.next_command().expect("hedge opening command");

        assert_eq!(session.state(), PulseSessionState::HedgeOpening);
        assert_eq!(session.actual_poly_fill_ratio(), 0.15);
        assert_eq!(cmd.hedge_reference_qty(), Decimal::new(1_500, 0));
    }

    #[test]
    fn pin_risk_protective_only_blocks_wider_delta_but_allows_smaller_one() {
        let mut session = test_session_with_gamma_mode(GammaCapMode::ProtectiveOnly);
        session.recompute_target_delta(0.40);
        session.recompute_target_delta(0.65);

        assert_eq!(session.target_event_delta(), 0.40);

        session.recompute_target_delta(0.20);
        assert_eq!(session.target_event_delta(), 0.20);
    }

    #[test]
    fn pin_risk_freeze_keeps_existing_target_delta_while_active() {
        let mut session = test_session_with_gamma_mode(GammaCapMode::Freeze);
        session.recompute_target_delta(0.40);
        session.recompute_target_delta(0.65);

        assert_eq!(session.target_event_delta(), 0.40);
    }
}
