use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use polyalpha_audit::{
    AuditEventKind, AuditEventPayload, PulseAssetHealthAuditEvent, PulseBookTapeAuditEvent,
    PulseLifecycleAuditEvent, PulseMarketTapeAuditEvent, PulseSessionSummaryRow,
    PulseSignalSnapshotAuditEvent,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulseSessionAuditSummary {
    pub session_id: String,
    pub final_state: String,
    pub deadline_exit_triggered: bool,
    pub audit_event_count: usize,
}

#[derive(Clone, Debug)]
pub struct PulseAuditRecord {
    pub timestamp_ms: u64,
    pub kind: AuditEventKind,
    pub payload: AuditEventPayload,
}

#[derive(Clone, Debug, Default)]
pub struct PulseAuditSink {
    records: Vec<PulseAuditRecord>,
    session_summaries: HashMap<String, PulseSessionAuditSummary>,
    warehouse_rows: HashMap<String, PulseSessionSummaryRow>,
}

impl PulseAuditSink {
    pub fn record_lifecycle(&mut self, event: PulseLifecycleAuditEvent) {
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseLifecycle,
            payload: AuditEventPayload::PulseLifecycle(event),
        });
    }

    pub fn record_asset_health(&mut self, event: PulseAssetHealthAuditEvent) {
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseAssetHealth,
            payload: AuditEventPayload::PulseAssetHealth(event),
        });
    }

    pub fn record_book_tape(&mut self, event: PulseBookTapeAuditEvent) {
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseBookTape,
            payload: AuditEventPayload::PulseBookTape(event),
        });
    }

    pub fn record_market_tape(&mut self, event: PulseMarketTapeAuditEvent) {
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseMarketTape,
            payload: AuditEventPayload::PulseMarketTape(event),
        });
    }

    pub fn record_signal_snapshot(&mut self, event: PulseSignalSnapshotAuditEvent) {
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseSignalSnapshot,
            payload: AuditEventPayload::PulseSignalSnapshot(event),
        });
    }

    pub fn finalize_session(
        &mut self,
        summary: PulseSessionAuditSummary,
        warehouse_row: PulseSessionSummaryRow,
    ) {
        self.session_summaries
            .insert(summary.session_id.clone(), summary);
        self.warehouse_rows.insert(
            warehouse_row.pulse_session_id.clone(),
            warehouse_row.clone(),
        );
        self.records.push(PulseAuditRecord {
            timestamp_ms: current_time_ms(),
            kind: AuditEventKind::PulseSessionSummary,
            payload: AuditEventPayload::PulseSessionSummary(warehouse_row),
        });
    }

    pub fn session_summary(&self, session_id: &str) -> Option<&PulseSessionAuditSummary> {
        self.session_summaries.get(session_id)
    }

    pub fn warehouse_row(&self, session_id: &str) -> Option<&PulseSessionSummaryRow> {
        self.warehouse_rows.get(session_id)
    }

    pub fn audit_event_count_for_session(&self, session_id: &str) -> usize {
        self.records
            .iter()
            .filter(|record| match &record.payload {
                AuditEventPayload::PulseLifecycle(event) => event.session_id == session_id,
                AuditEventPayload::PulseBookTape(event) => event.session_id == session_id,
                AuditEventPayload::PulseSessionSummary(event) => {
                    event.pulse_session_id == session_id
                }
                _ => false,
            })
            .count()
    }

    pub fn records(&self) -> &[PulseAuditRecord] {
        &self.records
    }

    pub fn warehouse_rows(&self) -> Vec<PulseSessionSummaryRow> {
        let mut rows = self.warehouse_rows.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|left, right| left.pulse_session_id.cmp(&right.pulse_session_id));
        rows
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use polyalpha_audit::{
        AuditGateResult, PulseAssetHealthAuditEvent, PulseBookSnapshotAudit,
        PulseBookTapeAuditEvent, PulseLifecycleAuditEvent, PulseMarketTapeAuditEvent,
        PulseSignalMode, PulseSignalSnapshotAuditEvent,
    };

    use super::*;

    #[test]
    fn audit_sink_tracks_lifecycle_and_summary_counts_per_session() {
        let mut sink = PulseAuditSink::default();
        sink.record_lifecycle(PulseLifecycleAuditEvent {
            session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            entry_price: Some("0.35".to_owned()),
            planned_poly_qty: "10000".to_owned(),
            actual_poly_filled_qty: "3500".to_owned(),
            actual_poly_fill_ratio: 0.35,
            actual_fill_notional_usd: "1225".to_owned(),
            candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
            expected_open_net_pnl_usd: "3.85".to_owned(),
            timeout_loss_estimate_usd: Some("21.68".to_owned()),
            required_hit_rate: Some(0.768),
            pulse_score_bps: Some(182.5),
            target_exit_price: Some("0.38".to_owned()),
            timeout_exit_price: Some("0.31".to_owned()),
            entry_executable_notional_usd: Some("250".to_owned()),
            reversion_pocket_ticks: Some(4.0),
            reversion_pocket_notional_usd: Some("28.57".to_owned()),
            vacuum_ratio: Some("1".to_owned()),
            effective_open: true,
            opening_outcome: "effective_open".to_owned(),
            opening_rejection_reason: None,
            opening_allocated_hedge_qty: "0.39".to_owned(),
            session_target_delta_exposure: "0.41".to_owned(),
            session_allocated_hedge_qty: "0.39".to_owned(),
            account_net_target_delta_before_order: "0.52".to_owned(),
            account_net_target_delta_after_order: "0.13".to_owned(),
            delta_bump_used: "10".to_owned(),
            anchor_latency_delta_ms: Some(18),
            distance_to_mid_bps: Some(7.5),
            relative_order_age_ms: Some(900),
            poly_yes_book: None,
            poly_no_book: None,
            cex_book: None,
        });
        sink.record_asset_health(PulseAssetHealthAuditEvent {
            asset: "btc".to_owned(),
            provider_id: Some("deribit_primary".to_owned()),
            anchor_age_ms: Some(42),
            anchor_latency_delta_ms: Some(18),
            poly_quote_age_ms: Some(15),
            cex_quote_age_ms: Some(8),
            open_sessions: 1,
            net_target_delta: Some("0.41".to_owned()),
            actual_exchange_position: Some("0.39".to_owned()),
            status: Some("healthy".to_owned()),
            disable_reason: None,
        });
        sink.record_book_tape(PulseBookTapeAuditEvent {
            session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            symbol: "btc-above-100k".to_owned(),
            book: PulseBookSnapshotAudit {
                exchange: "Binance".to_owned(),
                instrument: "cex_perp".to_owned(),
                received_at_ms: 3,
                sequence: 7,
                bids: Vec::new(),
                asks: Vec::new(),
            },
        });
        sink.finalize_session(
            PulseSessionAuditSummary {
                session_id: "pulse-session-1".to_owned(),
                final_state: "closed".to_owned(),
                deadline_exit_triggered: true,
                audit_event_count: 3,
            },
            PulseSessionSummaryRow {
                pulse_session_id: "pulse-session-1".to_owned(),
                asset: "btc".to_owned(),
                state: "closed".to_owned(),
                opened_at_ms: 1,
                closed_at_ms: Some(2),
                planned_poly_qty: "10000".to_owned(),
                actual_poly_filled_qty: "3500".to_owned(),
                actual_poly_fill_ratio: 0.35,
                entry_price: Some("0.35".to_owned()),
                actual_fill_notional_usd: "1225".to_owned(),
                candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
                expected_open_net_pnl_usd: "3.85".to_owned(),
                timeout_loss_estimate_usd: Some("21.68".to_owned()),
                required_hit_rate: Some(0.768),
                pulse_score_bps: Some(182.5),
                effective_open: true,
                opening_outcome: "effective_open".to_owned(),
                opening_rejection_reason: None,
                opening_allocated_hedge_qty: "0.39".to_owned(),
                session_target_delta_exposure: "0.41".to_owned(),
                session_allocated_hedge_qty: "0.39".to_owned(),
                net_edge_bps: Some(31.4),
                realized_pnl_usd: Some(75.7),
                exit_path: Some("maker_proxy_hit".to_owned()),
                target_exit_price: Some("0.38".to_owned()),
                final_exit_price: Some("0.38".to_owned()),
                timeout_exit_price: Some("0.31".to_owned()),
                entry_executable_notional_usd: Some("250".to_owned()),
                reversion_pocket_ticks: Some(4.0),
                reversion_pocket_notional_usd: Some("28.57".to_owned()),
                vacuum_ratio: Some("1".to_owned()),
                anchor_latency_delta_ms: Some(18),
                distance_to_mid_bps: Some(7.5),
                relative_order_age_ms: Some(900),
            },
        );

        assert_eq!(sink.audit_event_count_for_session("pulse-session-1"), 3);
        assert_eq!(
            sink.session_summary("pulse-session-1")
                .expect("summary")
                .final_state,
            "closed"
        );
        let warehouse_row = sink
            .warehouse_row("pulse-session-1")
            .expect("warehouse row");
        assert!(warehouse_row.effective_open);
        assert_eq!(warehouse_row.opening_outcome, "effective_open");
        assert_eq!(warehouse_row.opening_allocated_hedge_qty, "0.39");
    }

    #[test]
    fn audit_sink_tracks_market_tape_and_signal_snapshot_without_active_session() {
        let mut sink = PulseAuditSink::default();
        sink.record_market_tape(PulseMarketTapeAuditEvent {
            asset: "btc".to_owned(),
            symbol: "btc-above-100k".to_owned(),
            exchange: "Polymarket".to_owned(),
            instrument: "poly_no".to_owned(),
            exchange_timestamp_ms: 100,
            received_at_ms: 120,
            sequence: 7,
            best_bid: Some("0.38".to_owned()),
            best_ask: Some("0.39".to_owned()),
            mid: Some("0.385".to_owned()),
            last_trade_price: Some("0.39".to_owned()),
            bids: Vec::new(),
            asks: Vec::new(),
        });
        sink.record_signal_snapshot(PulseSignalSnapshotAuditEvent {
            asset: "btc".to_owned(),
            symbol: "btc-above-100k".to_owned(),
            mode_candidate: PulseSignalMode::DeepReversion,
            admission_result: AuditGateResult::Rejected,
            rejection_reason: Some("reachability_dead_zone".to_owned()),
            pulse_score_bps: 162.0,
            claim_price_move_bps: 180.0,
            fair_claim_move_bps: 12.0,
            cex_mid_move_bps: 8.0,
            swept_notional_usd: "500".to_owned(),
            swept_levels_count: 4,
            post_pulse_depth_gap_bps: 210.0,
            min_profitable_target_distance_bps: 540.0,
            reachability_cap_bps: 470.0,
            in_gray_zone: true,
            reachable: false,
            target_distance_to_mid_bps: None,
            predicted_hit_rate: None,
            maker_net_pnl_usd: None,
            timeout_net_pnl_usd: None,
            realizable_ev_usd: None,
            used_exchange_ts: true,
            native_sequence_present: false,
            post_sweep_update_count_5s: 1,
            max_interarrival_gap_ms_5s: 2600,
            observation_quality_score: Some(0.42),
            admission_eligible: false,
        });

        assert_eq!(sink.records().len(), 2);
        assert_eq!(sink.records()[0].kind, AuditEventKind::PulseMarketTape);
        assert!(matches!(
            sink.records()[0].payload,
            AuditEventPayload::PulseMarketTape(_)
        ));
        assert_eq!(sink.records()[1].kind, AuditEventKind::PulseSignalSnapshot);
        assert!(matches!(
            sink.records()[1].payload,
            AuditEventPayload::PulseSignalSnapshot(_)
        ));
    }
}
