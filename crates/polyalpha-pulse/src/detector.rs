use crate::model::{DetectorDecision, PulseFailureCode, PulseOpportunityInput};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PulseDetectorConfig {
    pub min_net_session_edge_bps: f64,
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
                rejection_code: Some(PulseFailureCode::AnchorQualityRejected),
            };
        }
        if !input.data_fresh {
            return DetectorDecision {
                should_trade: false,
                net_session_edge_bps: f64::NEG_INFINITY,
                rejection_code: Some(PulseFailureCode::DataFreshnessRejected),
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
                rejection_code: Some(PulseFailureCode::NetSessionEdgeBelowThreshold),
            };
        }

        DetectorDecision {
            should_trade: true,
            net_session_edge_bps,
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
        });

        assert!(!decision.should_trade);
        assert_eq!(
            decision.rejection_code.unwrap().as_str(),
            "net_session_edge_below_threshold"
        );
    }
}
