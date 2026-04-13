use polyalpha_core::{
    PulseAssetHealthRow, PulseMonitorView, PulseSessionDetailView, PulseSessionMonitorRow,
};

pub fn build_pulse_monitor_view(
    active_sessions: Vec<PulseSessionMonitorRow>,
    asset_health: Vec<PulseAssetHealthRow>,
    selected_session: Option<PulseSessionDetailView>,
) -> PulseMonitorView {
    PulseMonitorView {
        active_sessions,
        asset_health,
        selected_session,
    }
}

#[cfg(test)]
mod tests {
    use polyalpha_core::{PulseAssetHealthRow, PulseSessionMonitorRow};

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
            None,
        );

        assert_eq!(view.active_sessions.len(), 1);
        assert_eq!(view.asset_health.len(), 1);
        assert_eq!(view.active_sessions[0].asset, "btc");
    }
}
