use crate::model::{DetectorDecision, PulseFailureCode, PulseOpportunityInput};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PulseDetectorConfig {
    pub min_net_session_edge_bps: f64,
    pub min_claim_price_move_bps: f64,
    pub max_fair_claim_move_bps: f64,
    pub max_cex_mid_move_bps: f64,
    pub min_pulse_score_bps: f64,
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
        if !input.anchor_quality_ok {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps: f64::NEG_INFINITY,
                pulse_score_bps: f64::NEG_INFINITY,
                rejection_code: Some(PulseFailureCode::AnchorQualityRejected),
            };
        }
        if !input.data_fresh {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps: f64::NEG_INFINITY,
                pulse_score_bps: f64::NEG_INFINITY,
                rejection_code: Some(PulseFailureCode::DataFreshnessRejected),
            };
        }

        let pulse_score_bps = input.claim_price_move_bps - input.fair_claim_move_bps;
        if !input.has_pulse_history
            || input.claim_price_move_bps < self.config.min_claim_price_move_bps
            || input.fair_claim_move_bps > self.config.max_fair_claim_move_bps
            || input.cex_mid_move_bps > self.config.max_cex_mid_move_bps
            || pulse_score_bps < self.config.min_pulse_score_bps
        {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps: f64::NEG_INFINITY,
                pulse_score_bps,
                rejection_code: Some(PulseFailureCode::PulseConfirmationRejected),
            };
        }

        let friction_bps = input.poly_vwap_slippage_bps
            + input.hedge_slippage_bps
            + input.fee_bps
            + input.perp_basis_penalty_bps
            + input.rehedge_reserve_bps
            + input.timeout_exit_reserve_bps;
        let net_session_edge_bps = input.instant_basis_bps - friction_bps;

        if net_session_edge_bps < self.config.min_net_session_edge_bps {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps,
                pulse_score_bps,
                rejection_code: Some(PulseFailureCode::NetSessionEdgeBelowThreshold),
            };
        }

        DetectorDecision {
            should_trade: true,
            net_session_edge_bps,
            pulse_score_bps,
            rejection_code: None,
        }
    }
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
        }
    }

    #[test]
    fn detector_rejects_when_friction_exceeds_pulse_basis() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            instant_basis_bps: 31.0,
            poly_vwap_slippage_bps: 8.0,
            hedge_slippage_bps: 6.0,
            fee_bps: 3.0,
            perp_basis_penalty_bps: 5.0,
            rehedge_reserve_bps: 6.0,
            timeout_exit_reserve_bps: 5.0,
            anchor_quality_ok: true,
            data_fresh: true,
            has_pulse_history: true,
            claim_price_move_bps: 180.0,
            fair_claim_move_bps: 12.0,
            cex_mid_move_bps: 8.0,
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "net_session_edge_below_threshold"
        );
    }

    #[test]
    fn detector_rejects_value_gap_without_confirmed_pulse_setup() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            instant_basis_bps: 180.0,
            poly_vwap_slippage_bps: 8.0,
            hedge_slippage_bps: 6.0,
            fee_bps: 3.0,
            perp_basis_penalty_bps: 0.0,
            rehedge_reserve_bps: 5.0,
            timeout_exit_reserve_bps: 5.0,
            anchor_quality_ok: true,
            data_fresh: true,
            has_pulse_history: true,
            claim_price_move_bps: 35.0,
            fair_claim_move_bps: 12.0,
            cex_mid_move_bps: 8.0,
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "pulse_confirmation_rejected"
        );
    }

    #[test]
    fn detector_accepts_confirmed_pulse_when_poly_move_outruns_anchor_and_cex() {
        let detector = PulseDetector::new(detector_test_config(25.0));
        let decision = detector.evaluate(PulseOpportunityInput {
            instant_basis_bps: 180.0,
            poly_vwap_slippage_bps: 8.0,
            hedge_slippage_bps: 6.0,
            fee_bps: 3.0,
            perp_basis_penalty_bps: 0.0,
            rehedge_reserve_bps: 5.0,
            timeout_exit_reserve_bps: 5.0,
            anchor_quality_ok: true,
            data_fresh: true,
            has_pulse_history: true,
            claim_price_move_bps: 180.0,
            fair_claim_move_bps: 12.0,
            cex_mid_move_bps: 8.0,
        });

        assert!(decision.should_trade);
        assert_eq!(decision.rejection_code, None);
    }
}
