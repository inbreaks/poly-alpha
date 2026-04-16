use crate::model::{DetectorDecision, PulseFailureCode, PulseOpportunityInput};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PulseDetectorConfig {
    pub min_net_session_edge_bps: f64,
    pub min_claim_price_move_bps: f64,
    pub max_fair_claim_move_bps: f64,
    pub max_cex_mid_move_bps: f64,
    pub min_pulse_score_bps: f64,
    pub min_reversion_pocket_ticks: f64,
    pub max_timeout_loss_usd: Decimal,
    pub max_required_hit_rate: f64,
    pub min_expected_net_pnl_usd: Decimal,
    pub reachability_cap_bps: f64,
    pub hit_rate_safety_margin_pp: f64,
    pub min_realizable_ev_usd: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PulseDetector {
    config: PulseDetectorConfig,
}

impl PulseDetector {
    pub fn new(config: PulseDetectorConfig) -> Self {
        Self { config }
    }

    pub fn evaluate(&self, input: PulseOpportunityInput) -> DetectorDecision {
        if !input.anchor_quality_ok || !input.pricing_quality_ok {
            return rejected(PulseFailureCode::AnchorQualityRejected);
        }
        if !input.data_fresh {
            return rejected(PulseFailureCode::DataFreshnessRejected);
        }

        let pulse_score_bps = input.claim_price_move_bps - input.fair_claim_move_bps;
        if !input.has_pulse_history
            || input.claim_price_move_bps < self.config.min_claim_price_move_bps
            || input.fair_claim_move_bps > self.config.max_fair_claim_move_bps
            || input.cex_mid_move_bps > self.config.max_cex_mid_move_bps
            || pulse_score_bps < self.config.min_pulse_score_bps
        {
            return DetectorDecision {
                pulse_score_bps,
                ..rejected(PulseFailureCode::PulseConfirmationRejected)
            };
        }

        if input.reversion_pocket_ticks < self.config.min_reversion_pocket_ticks
            || input.vacuum_ratio <= Decimal::ZERO
        {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps: input.expected_net_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                required_hit_rate: 0.0,
                rejection_code: Some(PulseFailureCode::PulseConfirmationRejected),
            };
        }

        if input.min_profitable_target_distance_bps > self.config.reachability_cap_bps {
            return DetectorDecision {
                net_session_edge_bps: input.expected_net_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                ..rejected(PulseFailureCode::PulseConfirmationRejected)
            };
        }

        let net_session_edge_bps = input.expected_net_edge_bps;

        if net_session_edge_bps < self.config.min_net_session_edge_bps
            || input.expected_net_pnl_usd <= Decimal::ZERO
        {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                required_hit_rate: 0.0,
                rejection_code: Some(PulseFailureCode::NetSessionEdgeBelowThreshold),
            };
        }

        let required_hit_rate = required_hit_rate(
            input.expected_net_pnl_usd,
            input.timeout_loss_estimate_usd,
            self.config.min_expected_net_pnl_usd,
        );

        let Some(predicted_hit_rate) = input.predicted_hit_rate else {
            return DetectorDecision {
                net_session_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                required_hit_rate,
                rejection_code: Some(PulseFailureCode::PulseConfirmationRejected),
                should_trade: false,
            };
        };
        let Some(realizable_ev_usd) = input.realizable_ev_usd else {
            return DetectorDecision {
                net_session_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                required_hit_rate,
                rejection_code: Some(PulseFailureCode::NetSessionEdgeBelowThreshold),
                should_trade: false,
            };
        };
        let required_with_margin = required_hit_rate + self.config.hit_rate_safety_margin_pp / 100.0;

        if input.timeout_loss_estimate_usd > self.config.max_timeout_loss_usd
            || required_hit_rate > self.config.max_required_hit_rate
            || predicted_hit_rate < required_with_margin
            || realizable_ev_usd < self.config.min_realizable_ev_usd
        {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps,
                expected_net_pnl_usd: input.expected_net_pnl_usd,
                pulse_score_bps,
                required_hit_rate,
                rejection_code: Some(PulseFailureCode::TimeoutRiskRejected),
            };
        }

        DetectorDecision {
            should_trade: true,
            net_session_edge_bps,
            expected_net_pnl_usd: input.expected_net_pnl_usd,
            pulse_score_bps,
            required_hit_rate,
            rejection_code: None,
        }
    }
}

fn rejected(code: PulseFailureCode) -> DetectorDecision {
    DetectorDecision {
        should_trade: false,
        net_session_edge_bps: f64::NEG_INFINITY,
        expected_net_pnl_usd: Decimal::ZERO,
        pulse_score_bps: f64::NEG_INFINITY,
        required_hit_rate: 0.0,
        rejection_code: Some(code),
    }
}

fn required_hit_rate(
    expected_net_pnl_usd: Decimal,
    timeout_loss_estimate_usd: Decimal,
    min_expected_net_pnl_usd: Decimal,
) -> f64 {
    if timeout_loss_estimate_usd <= Decimal::ZERO {
        return 0.0;
    }
    if expected_net_pnl_usd <= Decimal::ZERO {
        return f64::INFINITY;
    }

    let numerator = timeout_loss_estimate_usd + min_expected_net_pnl_usd.max(Decimal::ZERO);
    let denominator = expected_net_pnl_usd + timeout_loss_estimate_usd;
    if denominator <= Decimal::ZERO {
        return f64::INFINITY;
    }

    (numerator / denominator)
        .to_f64()
        .filter(|value| value.is_finite())
        .unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detector_test_config(min_net_session_edge_bps: f64) -> PulseDetectorConfig {
        PulseDetectorConfig {
            min_net_session_edge_bps,
            min_claim_price_move_bps: 80.0,
            max_fair_claim_move_bps: 35.0,
            max_cex_mid_move_bps: 30.0,
            min_pulse_score_bps: 50.0,
            min_reversion_pocket_ticks: 1.0,
            max_timeout_loss_usd: Decimal::new(20, 0),
            max_required_hit_rate: 0.70,
            min_expected_net_pnl_usd: Decimal::new(50, 2),
            reachability_cap_bps: 500.0,
            hit_rate_safety_margin_pp: 10.0,
            min_realizable_ev_usd: Decimal::ONE,
        }
    }

    fn base_input() -> PulseOpportunityInput {
        PulseOpportunityInput {
            instant_basis_bps: 180.0,
            poly_vwap_slippage_bps: 8.0,
            hedge_slippage_bps: 6.0,
            fee_bps: 3.0,
            perp_basis_penalty_bps: 0.0,
            rehedge_reserve_bps: 5.0,
            timeout_exit_reserve_bps: 5.0,
            expected_net_edge_bps: 80.0,
            expected_net_pnl_usd: Decimal::new(300, 2),
            timeout_loss_estimate_usd: Decimal::new(5, 0),
            min_profitable_target_distance_bps: 118.0,
            target_distance_to_mid_bps: Some(118.0),
            predicted_hit_rate: Some(0.80),
            realizable_ev_usd: Some(Decimal::new(3, 0)),
            reversion_pocket_ticks: 3.0,
            vacuum_ratio: Decimal::ONE,
            anchor_quality_ok: true,
            pricing_quality_ok: true,
            data_fresh: true,
            has_pulse_history: true,
            claim_price_move_bps: 180.0,
            fair_claim_move_bps: 12.0,
            cex_mid_move_bps: 8.0,
        }
    }

    #[test]
    fn detector_rejects_when_friction_exceeds_pulse_basis() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            instant_basis_bps: 31.0,
            perp_basis_penalty_bps: 5.0,
            rehedge_reserve_bps: 6.0,
            expected_net_edge_bps: 0.0,
            expected_net_pnl_usd: Decimal::ZERO,
            timeout_loss_estimate_usd: Decimal::new(5, 0),
            reversion_pocket_ticks: 2.0,
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "net_session_edge_below_threshold"
        );
        assert_eq!(decision.expected_net_pnl_usd, Decimal::ZERO);
    }

    #[test]
    fn detector_rejects_value_gap_without_confirmed_pulse_setup() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_edge_bps: 0.0,
            expected_net_pnl_usd: Decimal::ZERO,
            reversion_pocket_ticks: 2.0,
            claim_price_move_bps: 35.0,
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "pulse_confirmation_rejected"
        );
        assert_eq!(decision.expected_net_pnl_usd, Decimal::ZERO);
    }

    #[test]
    fn detector_accepts_confirmed_pulse_when_poly_move_outruns_anchor_and_cex() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_edge_bps: 120.0,
            expected_net_pnl_usd: Decimal::new(3, 0),
            reversion_pocket_ticks: 2.0,
            timeout_loss_estimate_usd: Decimal::new(2, 0),
            ..base_input()
        });

        assert!(decision.should_trade);
        assert_eq!(decision.rejection_code, None);
        assert_eq!(decision.expected_net_pnl_usd, Decimal::new(3, 0));
    }

    #[test]
    fn detector_rejects_positive_anchor_gap_when_executable_session_edge_is_negative() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_edge_bps: -12.0,
            expected_net_pnl_usd: Decimal::new(-25, 2),
            timeout_loss_estimate_usd: Decimal::new(5, 0),
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "net_session_edge_below_threshold"
        );
        assert_eq!(decision.expected_net_pnl_usd, Decimal::new(-25, 2));
    }

    #[test]
    fn detector_rejects_when_reversion_pocket_is_too_shallow() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_pnl_usd: Decimal::new(250, 2),
            reversion_pocket_ticks: 0.5,
            vacuum_ratio: Decimal::new(2, 1),
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "pulse_confirmation_rejected"
        );
        assert_eq!(decision.expected_net_pnl_usd, Decimal::new(250, 2));
    }

    #[test]
    fn detector_accepts_only_when_session_edge_and_pocket_both_pass() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_pnl_usd: Decimal::new(250, 2),
            vacuum_ratio: Decimal::new(2, 0),
            timeout_loss_estimate_usd: Decimal::new(2, 0),
            ..base_input()
        });

        assert!(decision.should_trade);
        assert_eq!(decision.rejection_code, None);
        assert_eq!(decision.expected_net_pnl_usd, Decimal::new(250, 2));
    }

    #[test]
    fn detector_rejects_when_timeout_loss_requires_unrealistic_hit_rate() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            expected_net_edge_bps: 120.0,
            expected_net_pnl_usd: Decimal::new(720, 2),
            timeout_loss_estimate_usd: Decimal::new(2168, 2),
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "timeout_risk_rejected"
        );
        assert!((decision.required_hit_rate - 0.7673).abs() < 0.001);
    }

    #[test]
    fn detector_rejects_when_min_profitable_target_distance_exceeds_reachability_cap() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            min_profitable_target_distance_bps: 540.0,
            target_distance_to_mid_bps: None,
            predicted_hit_rate: None,
            realizable_ev_usd: None,
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "pulse_confirmation_rejected"
        );
    }

    #[test]
    fn detector_rejects_when_predicted_hit_rate_does_not_clear_required_plus_margin() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            min_profitable_target_distance_bps: 118.0,
            target_distance_to_mid_bps: Some(118.0),
            predicted_hit_rate: Some(0.41),
            realizable_ev_usd: Some(Decimal::new(162, 2)),
            timeout_loss_estimate_usd: Decimal::new(310, 2),
            expected_net_pnl_usd: Decimal::new(472, 2),
            ..base_input()
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "timeout_risk_rejected"
        );
    }
}
