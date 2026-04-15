use polyalpha_core::{
    PulseAssetHealthRow, PulseMarketMonitorRow, PulseMonitorView, PulseSessionDetailView,
    PulseSessionMonitorRow,
};

pub fn build_pulse_monitor_view(
    active_sessions: Vec<PulseSessionMonitorRow>,
    asset_health: Vec<PulseAssetHealthRow>,
    markets: Vec<PulseMarketMonitorRow>,
    selected_session: Option<PulseSessionDetailView>,
) -> PulseMonitorView {
    PulseMonitorView {
        active_sessions,
        asset_health,
        markets,
        selected_session,
    }
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{PulseAssetHealthRow, PulseMarketMonitorRow, PulseSessionMonitorRow};

    use super::*;

    #[test]
    fn build_pulse_monitor_view_keeps_session_and_health_rows() {
        let view = build_pulse_monitor_view(
            vec![PulseSessionMonitorRow {
                session_id: "pulse-session-1".to_owned(),
                asset: "btc".to_owned(),
                state: "maker_exit_working".to_owned(),
                remaining_secs: 540,
                net_edge_bps: 31.4,
            }],
            vec![PulseAssetHealthRow {
                asset: "btc".to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(15),
                cex_quote_age_ms: Some(8),
                open_sessions: 1,
                net_target_delta: Some(0.41),
                actual_exchange_position: Some(0.39),
                status: Some("healthy".to_owned()),
                disable_reason: None,
            }],
            vec![PulseMarketMonitorRow {
                symbol: "btc-above-100k".to_owned(),
                asset: "btc".to_owned(),
                claim_side: Some("NO".to_owned()),
                pulse_score_bps: Some(182.5),
                net_edge_bps: Some(97.4),
                poly_yes_price: Some(0.645),
                poly_no_price: Some(0.355),
                cex_price: Some(100_005.0),
                fair_prob_yes: Some(0.602),
                entry_price: Some(0.356),
                target_exit_price: Some(0.38),
                timeout_exit_price: Some(0.31),
                expected_net_pnl_usd: Some(3.85),
                timeout_loss_estimate_usd: Some(21.68),
                required_hit_rate: Some(0.768),
                reversion_pocket_ticks: Some(4.0),
                reversion_pocket_notional_usd: Some(28.57),
                vacuum_ratio: Some(1.0),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(15),
                cex_quote_age_ms: Some(8),
                open_sessions: 1,
                status: "ready".to_owned(),
                disable_reason: None,
            }],
            None,
        );

        assert_eq!(view.active_sessions.len(), 1);
        assert_eq!(view.asset_health.len(), 1);
        assert_eq!(view.markets.len(), 1);
        assert_eq!(view.active_sessions[0].asset, "btc");
    }
}
