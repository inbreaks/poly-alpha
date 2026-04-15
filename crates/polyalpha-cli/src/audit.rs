use anyhow::{anyhow, Context, Result};
use chrono::{Local, TimeZone};
use libm::erf;
use polyalpha_audit::{
    AuditEvent, AuditEventPayload, AuditReader, AuditSessionSummary, AuditWarehouse,
    MarketMarkEvent, PositionMarkEvent, PulseSessionSummaryRow, SignalEmittedEvent,
};
use polyalpha_core::{
    Exchange, ExecutionEvent, MarketConfig, MarketRule, MarketRuleKind, OrderSide,
    RecoveryDecisionReason, Settings, TokenSide, TradePlan, TradeView, VenueQuantity,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::args::{AuditCommand, AuditOutputFormat};

#[derive(Serialize)]
struct AuditEventView {
    timestamp_ms: u64,
    time: String,
    kind: String,
    summary: String,
    symbol: Option<String>,
    signal_id: Option<String>,
    correlation_id: Option<String>,
    gate: Option<String>,
    result: Option<String>,
    reason: Option<String>,
}

#[derive(Serialize)]
struct MarketAuditReport {
    summary: AuditSessionSummary,
    symbol: String,
    latest_market_mark: Option<MarketMarkEvent>,
    latest_position_mark: Option<PositionMarkEvent>,
    recent_events: Vec<AuditEventView>,
}

#[derive(Serialize)]
struct PositionExplainReport {
    summary: AuditSessionSummary,
    symbol: String,
    latest_signal: Option<SignalEmittedEvent>,
    gate_chain: Vec<AuditEventView>,
    executions: Vec<AuditEventView>,
    latest_position_mark: Option<PositionMarkEvent>,
    latest_closed_trade: Option<TradeView>,
    recent_events: Vec<AuditEventView>,
}

#[derive(Serialize)]
struct AuditEventQueryReport {
    summary: AuditSessionSummary,
    filters: AuditEventQueryFiltersView,
    events: Vec<AuditEventView>,
}

#[derive(Serialize)]
struct PulseSessionsReport {
    summary: AuditSessionSummary,
    asset_filter: Option<String>,
    raw_opening_kpi: PulseRawOpeningKpi,
    sessions: Vec<PulseSessionSummaryRow>,
}

#[derive(Serialize)]
struct PulseRawOpeningKpi {
    opening_attempts: usize,
    positive_fill_count: usize,
    effective_open_count: usize,
    effective_open_rate: f64,
    still_active_effective_open_count: usize,
    closed_rejection_reason_counts: HashMap<String, u64>,
    max_actual_fill_notional_usd: Option<String>,
    max_expected_open_net_pnl_usd: Option<String>,
    effective_open_examples: Vec<PulseRawOpeningExample>,
}

#[derive(Clone, Serialize)]
struct PulseRawOpeningExample {
    session_id: String,
    asset: String,
    state: String,
    actual_fill_notional_usd: String,
    expected_open_net_pnl_usd: String,
    opening_allocated_hedge_qty: String,
    anchor_latency_delta_ms: Option<u64>,
}

#[derive(Clone, Default)]
struct PulseRawOpeningSessionAggregate {
    asset: String,
    latest_seq: u64,
    latest_state: String,
    latest_actual_fill_notional_usd: Decimal,
    latest_expected_open_net_pnl_usd: Decimal,
    latest_opening_allocated_hedge_qty: String,
    latest_anchor_latency_delta_ms: Option<u64>,
    latest_opening_rejection_reason: Option<String>,
    positive_fill: bool,
    effective_open: bool,
}

#[derive(Serialize)]
struct PulseSessionDetailAuditReport {
    summary: AuditSessionSummary,
    session: PulseSessionSummaryRow,
    timeline: Vec<PulseSessionTimelineItem>,
}

#[derive(Clone, Serialize)]
struct PulseSessionTimelineItem {
    seq: u64,
    timestamp_ms: u64,
    time: String,
    kind: String,
    state: String,
    summary: String,
    instrument: Option<String>,
}

#[derive(Serialize)]
struct AuditEventQueryFiltersView {
    symbol: Option<String>,
    kind: Option<String>,
    gate: Option<String>,
    result: Option<String>,
    reason: Option<String>,
    query: Option<String>,
    limit: usize,
}

#[derive(Clone, Serialize)]
struct TradeTimelineListItem {
    trade: usize,
    seq: u64,
    timestamp_ms: u64,
    time: String,
    symbol: String,
    realized_pnl_usd: String,
    path: String,
    correlation_id: String,
}

#[derive(Serialize)]
struct TradeTimelineListReport {
    summary: AuditSessionSummary,
    symbol_filter: Option<String>,
    trades: Vec<TradeTimelineListItem>,
}

#[derive(Serialize)]
struct TradeTimelinePreCloseView {
    seq: u64,
    timestamp_ms: u64,
    time: String,
    tick_index: u64,
    poly_side: String,
    poly_quantity: String,
    poly_entry_price: String,
    poly_current_price: String,
    cex_exchange: String,
    cex_side: String,
    cex_quantity: String,
    cex_entry_price: String,
    cex_current_price: String,
    delta: String,
    total_pnl_usd: String,
}

#[derive(Clone, Serialize)]
struct TradeTimelineRow {
    seq: u64,
    timestamp_ms: u64,
    time: String,
    stage: String,
    action: String,
    quantity: String,
    price: String,
    edge_usd: String,
    residual_delta: String,
    note: String,
}

#[derive(Serialize)]
struct TradeTimelineAttributionView {
    close_chain_loss_usd: String,
    pre_close_floating_loss_realized_usd: String,
    pre_close_mark_pnl_usd: Option<String>,
}

#[derive(Serialize)]
struct TradeTimelineDetailReport {
    summary: AuditSessionSummary,
    trade: TradeTimelineListItem,
    skew_detected: bool,
    pre_close: Option<TradeTimelinePreCloseView>,
    rows: Vec<TradeTimelineRow>,
    attribution: Option<TradeTimelineAttributionView>,
}

#[derive(Clone, Serialize)]
struct OpenDecisionListItem {
    entry: usize,
    seq: u64,
    timestamp_ms: u64,
    time: String,
    symbol: String,
    token_side: String,
    signal_price: Option<f64>,
    fair_value: f64,
    raw_mispricing: f64,
    delta_estimate: f64,
    planned_edge_usd: String,
    correlation_id: String,
}

#[derive(Serialize)]
struct OpenDecisionListReport {
    summary: AuditSessionSummary,
    symbol_filter: Option<String>,
    entries: Vec<OpenDecisionListItem>,
}

#[derive(Serialize)]
struct OpenDecisionMarketView {
    question: Option<String>,
    rule_kind: String,
    lower_strike: Option<String>,
    upper_strike: Option<String>,
    settlement_timestamp_ms: u64,
    settlement_time: String,
}

#[derive(Serialize)]
struct OpenDecisionSignalView {
    signal_id: String,
    time: String,
    token_side: String,
    direction: String,
    candidate_price: Option<f64>,
    fair_value: f64,
    raw_mispricing: f64,
    delta_estimate: f64,
    z_score: Option<f64>,
    cex_reference_price: Option<f64>,
    minutes_to_expiry: Option<f64>,
    raw_sigma_annualized: Option<f64>,
    effective_sigma_annualized: Option<f64>,
    sigma_source: Option<String>,
    returns_window_len: usize,
    implied_sigma_annualized: Option<f64>,
    evaluable_status: String,
    async_classification: String,
    poly_quote_age_ms: Option<u64>,
    cex_quote_age_ms: Option<u64>,
    cross_leg_skew_ms: Option<u64>,
}

#[derive(Serialize)]
struct OpenDecisionLatestMarketView {
    seq: u64,
    tick_index: u64,
    time: String,
    active_token_side: Option<String>,
    poly_price: Option<f64>,
    cex_price: Option<f64>,
    fair_value: Option<f64>,
    minutes_to_expiry: Option<f64>,
    raw_sigma_annualized: Option<f64>,
    effective_sigma_annualized: Option<f64>,
    sigma_source: Option<String>,
    returns_window_len: usize,
    implied_sigma_annualized: Option<f64>,
    evaluable_status: String,
    async_classification: String,
}

#[derive(Serialize)]
struct OpenDecisionPlanView {
    seq: u64,
    time: String,
    stage: String,
    plan_id: String,
    parent_plan_id: Option<String>,
    poly_side: String,
    poly_token_side: String,
    poly_planned_shares: String,
    poly_executable_price: String,
    cex_side: String,
    cex_planned_qty: String,
    cex_executable_price: String,
    planned_edge_usd: String,
    instant_open_loss_usd: String,
    poly_avg_vs_signal_price_bps: Option<f64>,
    post_rounding_residual_delta: f64,
    shock_loss_up_2pct: String,
    max_residual_delta: f64,
    max_shock_loss_usd: String,
}

#[derive(Serialize)]
struct OpenDecisionExecutionView {
    seq: u64,
    time: String,
    stage: String,
    plan_id: String,
    status: String,
    realized_edge_usd: String,
    actual_residual_delta: f64,
    recovery_required: bool,
    poly_filled_qty: String,
    cex_filled_qty: String,
}

#[derive(Serialize)]
struct OpenDecisionDetailReport {
    summary: AuditSessionSummary,
    entry: OpenDecisionListItem,
    market: OpenDecisionMarketView,
    signal: Option<OpenDecisionSignalView>,
    latest_market_snapshot: Option<OpenDecisionLatestMarketView>,
    gate_chain: Vec<AuditEventView>,
    plans: Vec<OpenDecisionPlanView>,
    executions: Vec<OpenDecisionExecutionView>,
}

pub fn run_audit_command(command: AuditCommand) -> Result<()> {
    match command {
        AuditCommand::Events {
            env,
            session_id,
            symbol,
            kind,
            gate,
            result,
            reason,
            query,
            limit,
            format,
        } => run_events_report(
            &env,
            session_id.as_deref(),
            AuditEventQueryFiltersView {
                symbol,
                kind,
                gate,
                result,
                reason,
                query,
                limit,
            },
            limit,
            format,
        ),
        AuditCommand::SessionSummary {
            env,
            session_id,
            format,
        } => run_session_summary(&env, session_id.as_deref(), format),
        AuditCommand::PulseSessions {
            env,
            session_id,
            asset,
            format,
        } => run_pulse_sessions_report(&env, session_id.as_deref(), asset.as_deref(), format),
        AuditCommand::PulseSessionDetail {
            env,
            session_id,
            pulse_session_id,
            format,
        } => {
            run_pulse_session_detail_report(&env, session_id.as_deref(), &pulse_session_id, format)
        }
        AuditCommand::Market {
            env,
            session_id,
            symbol,
            limit,
            format,
        } => run_market_report(&env, session_id.as_deref(), &symbol, limit, format),
        AuditCommand::PositionExplain {
            env,
            session_id,
            symbol,
            limit,
            format,
        } => run_position_explain(&env, session_id.as_deref(), &symbol, limit, format),
        AuditCommand::TradeTimeline {
            env,
            session_id,
            symbol,
            correlation_id,
            trade,
            format,
        } => run_trade_timeline(
            &env,
            session_id.as_deref(),
            symbol.as_deref(),
            correlation_id.as_deref(),
            trade,
            format,
        ),
        AuditCommand::OpenDecision {
            env,
            session_id,
            symbol,
            correlation_id,
            entry,
            format,
        } => run_open_decision(
            &env,
            session_id.as_deref(),
            symbol.as_deref(),
            correlation_id.as_deref(),
            entry,
            format,
        ),
    }
}

fn run_trade_timeline(
    env: &str,
    session_id: Option<&str>,
    symbol: Option<&str>,
    correlation_id: Option<&str>,
    trade: Option<usize>,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let trades = collect_closed_trade_items(&events, symbol);

    match select_closed_trade(&trades, correlation_id, trade)? {
        Some(selected_trade) => {
            let report = build_trade_timeline_detail(&events, selected_trade)?;
            match format {
                AuditOutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&TradeTimelineDetailReport { summary, ..report })?
                ),
                AuditOutputFormat::Table => {
                    print_trade_timeline_detail(&TradeTimelineDetailReport { summary, ..report })
                }
            }
        }
        None => {
            let report = TradeTimelineListReport {
                summary,
                symbol_filter: symbol.map(str::to_owned),
                trades,
            };
            match format {
                AuditOutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                AuditOutputFormat::Table => print_trade_timeline_list(&report),
            }
        }
    }

    Ok(())
}

fn run_open_decision(
    env: &str,
    session_id: Option<&str>,
    symbol: Option<&str>,
    correlation_id: Option<&str>,
    entry: Option<usize>,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let entries = collect_open_decision_items(&events, symbol);

    match select_open_decision(&entries, correlation_id, entry)? {
        Some(selected_entry) => {
            let report = build_open_decision_detail(&events, &settings.markets, selected_entry)?;
            match format {
                AuditOutputFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&OpenDecisionDetailReport { summary, ..report })?
                ),
                AuditOutputFormat::Table => {
                    print_open_decision_detail(&OpenDecisionDetailReport { summary, ..report })
                }
            }
        }
        None => {
            let report = OpenDecisionListReport {
                summary,
                symbol_filter: symbol.map(str::to_owned),
                entries,
            };
            match format {
                AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                AuditOutputFormat::Table => print_open_decision_list(&report),
            }
        }
    }

    Ok(())
}

fn collect_closed_trade_items(
    events: &[AuditEvent],
    symbol_filter: Option<&str>,
) -> Vec<TradeTimelineListItem> {
    let mut trades = Vec::new();

    for event in events {
        let Some(ExecutionEvent::TradeClosed {
            symbol,
            correlation_id,
            realized_pnl,
            ..
        }) = execution_event(event)
        else {
            continue;
        };

        if symbol_filter.is_some_and(|filter| !symbol_matches(&symbol.0, filter)) {
            continue;
        }

        trades.push(TradeTimelineListItem {
            trade: trades.len() + 1,
            seq: event.seq,
            timestamp_ms: event.timestamp_ms,
            time: format_timestamp_ms(event.timestamp_ms),
            symbol: symbol.0.clone(),
            realized_pnl_usd: format_decimal(realized_pnl.0),
            path: summarize_trade_path(events, correlation_id),
            correlation_id: correlation_id.clone(),
        });
    }

    trades
}

fn select_closed_trade<'a>(
    trades: &'a [TradeTimelineListItem],
    correlation_id: Option<&str>,
    trade: Option<usize>,
) -> Result<Option<&'a TradeTimelineListItem>> {
    if correlation_id.is_none() && trade.is_none() {
        return Ok(None);
    }

    if let Some(correlation_id) = correlation_id {
        return trades
            .iter()
            .find(|item| item.correlation_id == correlation_id)
            .map(Some)
            .ok_or_else(|| anyhow!("未找到 correlation_id=`{correlation_id}` 对应的已平仓交易"));
    }

    let trade = trade.expect("trade checked above");
    if trade == 0 {
        return Err(anyhow!("`--trade` 从 1 开始计数"));
    }

    trades
        .iter()
        .find(|item| item.trade == trade)
        .map(Some)
        .ok_or_else(|| anyhow!("未找到第 {trade} 笔已平仓交易"))
}

fn summarize_trade_path(events: &[AuditEvent], correlation_id: &str) -> String {
    let mut labels = Vec::new();

    for event in events.iter().filter(|event| {
        event.correlation_id.as_deref() == Some(correlation_id)
            && matches!(event.kind, polyalpha_audit::AuditEventKind::Execution)
    }) {
        let Some(label) = (match execution_event(event) {
            Some(ExecutionEvent::TradePlanCreated { plan }) => {
                Some(trade_plan_stage_label(plan).to_owned())
            }
            Some(ExecutionEvent::RecoveryPlanCreated { .. }) => Some("处理剩余敞口".to_owned()),
            _ => None,
        }) else {
            continue;
        };

        if labels.last() != Some(&label) {
            labels.push(label);
        }
    }

    if labels.is_empty() {
        "未识别平仓链".to_owned()
    } else {
        labels.join(" -> ")
    }
}

fn build_trade_timeline_detail(
    events: &[AuditEvent],
    selected_trade: &TradeTimelineListItem,
) -> Result<TradeTimelineDetailReport> {
    let mut related_events = events
        .iter()
        .filter(|event| {
            event.correlation_id.as_deref() == Some(selected_trade.correlation_id.as_str())
                && matches!(event.kind, polyalpha_audit::AuditEventKind::Execution)
        })
        .collect::<Vec<_>>();
    related_events.sort_by_key(|event| event.seq);

    if related_events.is_empty() {
        return Err(anyhow!(
            "未找到 correlation_id=`{}` 对应的执行事件",
            selected_trade.correlation_id
        ));
    }

    let first_seq = related_events
        .first()
        .map(|event| event.seq)
        .unwrap_or_default();
    let pre_close_event = events.iter().rev().find(|event| {
        event.seq < first_seq
            && matches!(event.kind, polyalpha_audit::AuditEventKind::PositionMark)
            && event
                .symbol
                .as_deref()
                .is_some_and(|symbol| symbol_matches(symbol, &selected_trade.symbol))
    });
    let pre_close = pre_close_event.and_then(pre_close_snapshot_view);

    let skew_detected = related_events
        .windows(2)
        .any(|window| window[1].timestamp_ms < window[0].timestamp_ms);

    let mut rows = Vec::new();
    if let Some(pre_close) = &pre_close {
        rows.push(TradeTimelineRow {
            seq: pre_close.seq,
            timestamp_ms: pre_close.timestamp_ms,
            time: pre_close.time.clone(),
            stage: "平仓前快照".to_owned(),
            action: "持仓状态".to_owned(),
            quantity: "-".to_owned(),
            price: "-".to_owned(),
            edge_usd: "-".to_owned(),
            residual_delta: pre_close.delta.clone(),
            note: format!("当前总盈亏 {}", pre_close.total_pnl_usd),
        });
    }

    let mut plan_stage_by_plan_id = HashMap::new();
    let mut close_chain_loss = Decimal::ZERO;

    for event in related_events {
        let Some(execution) = execution_event(event) else {
            continue;
        };
        match execution {
            ExecutionEvent::TradePlanCreated { plan } => {
                let stage = trade_plan_stage_label(plan).to_owned();
                plan_stage_by_plan_id.insert(plan.plan_id.clone(), stage.clone());
                rows.push(plan_timeline_row(event, plan, &stage, false));
            }
            ExecutionEvent::RecoveryPlanCreated { plan } => {
                let stage = "处理剩余敞口".to_owned();
                plan_stage_by_plan_id.insert(plan.plan_id.clone(), stage.clone());
                rows.push(plan_timeline_row(event, plan, &stage, true));
            }
            ExecutionEvent::OrderFilled(fill) => {
                rows.push(fill_timeline_row(event, fill));
            }
            ExecutionEvent::ExecutionResultRecorded { result } => {
                close_chain_loss += result.realized_edge_usd.0;
                let stage = plan_stage_by_plan_id
                    .get(&result.plan_id)
                    .map(|stage| format!("{stage}结果"))
                    .unwrap_or_else(|| "执行结果".to_owned());
                rows.push(result_timeline_row(event, &stage, result));
            }
            ExecutionEvent::TradeClosed { realized_pnl, .. } => {
                rows.push(TradeTimelineRow {
                    seq: event.seq,
                    timestamp_ms: event.timestamp_ms,
                    time: format_timestamp_ms(event.timestamp_ms),
                    stage: "平仓完成".to_owned(),
                    action: "已实现盈亏".to_owned(),
                    quantity: "-".to_owned(),
                    price: "-".to_owned(),
                    edge_usd: format_decimal(realized_pnl.0),
                    residual_delta: "0".to_owned(),
                    note: "-".to_owned(),
                });
            }
            _ => {}
        }
    }

    let realized_pnl = selected_trade
        .realized_pnl_usd
        .parse::<Decimal>()
        .unwrap_or(Decimal::ZERO);
    let attribution = Some(TradeTimelineAttributionView {
        close_chain_loss_usd: format_decimal(close_chain_loss),
        pre_close_floating_loss_realized_usd: format_decimal(realized_pnl - close_chain_loss),
        pre_close_mark_pnl_usd: pre_close.as_ref().map(|mark| mark.total_pnl_usd.clone()),
    });

    Ok(TradeTimelineDetailReport {
        summary: AuditSessionSummary {
            session_id: String::new(),
            env: String::new(),
            mode: Default::default(),
            status: Default::default(),
            started_at_ms: 0,
            ended_at_ms: None,
            updated_at_ms: 0,
            market_count: 0,
            markets: Vec::new(),
            counters: Default::default(),
            latest_tick_index: 0,
            latest_checkpoint_ms: None,
            latest_equity: None,
            latest_total_pnl_usd: None,
            latest_today_pnl_usd: None,
            latest_total_exposure_usd: None,
            rejection_reasons: HashMap::new(),
            skip_reasons: HashMap::new(),
            evaluable_status_counts: HashMap::new(),
            async_classification_counts: HashMap::new(),
            pulse_session_summaries: Vec::new(),
        },
        trade: selected_trade.clone(),
        skew_detected,
        pre_close,
        rows,
        attribution,
    })
}

fn collect_open_decision_items(
    events: &[AuditEvent],
    symbol_filter: Option<&str>,
) -> Vec<OpenDecisionListItem> {
    let mut entries = Vec::new();

    for event in events {
        let Some(ExecutionEvent::TradePlanCreated { plan }) = execution_event(event) else {
            continue;
        };
        if plan.intent_type != "open_position" {
            continue;
        }
        if symbol_filter.is_some_and(|filter| !symbol_matches(&plan.symbol.0, filter)) {
            continue;
        }

        let signal = events.iter().find_map(|candidate_event| {
            (candidate_event.correlation_id.as_deref() == Some(plan.correlation_id.as_str()))
                .then_some(candidate_event)
                .and_then(signal_event)
        });
        let (token_side, signal_price, fair_value, raw_mispricing, delta_estimate) =
            if let Some(signal) = signal {
                (
                    format_token_side_label(signal.candidate.token_side).to_owned(),
                    observed_candidate_price(signal),
                    signal.candidate.fair_value,
                    signal.candidate.raw_mispricing,
                    signal.candidate.delta_estimate,
                )
            } else {
                (
                    format_token_side_label(plan.poly_token_side).to_owned(),
                    None,
                    0.0,
                    0.0,
                    0.0,
                )
            };

        entries.push(OpenDecisionListItem {
            entry: entries.len() + 1,
            seq: event.seq,
            timestamp_ms: event.timestamp_ms,
            time: format_timestamp_ms(event.timestamp_ms),
            symbol: plan.symbol.0.clone(),
            token_side,
            signal_price,
            fair_value,
            raw_mispricing,
            delta_estimate,
            planned_edge_usd: format_decimal(plan.planned_edge_usd.0),
            correlation_id: plan.correlation_id.clone(),
        });
    }

    entries
}

fn select_open_decision<'a>(
    entries: &'a [OpenDecisionListItem],
    correlation_id: Option<&str>,
    entry: Option<usize>,
) -> Result<Option<&'a OpenDecisionListItem>> {
    if correlation_id.is_none() && entry.is_none() {
        return Ok(None);
    }

    if let Some(correlation_id) = correlation_id {
        return entries
            .iter()
            .find(|item| item.correlation_id == correlation_id)
            .map(Some)
            .ok_or_else(|| anyhow!("未找到 correlation_id=`{correlation_id}` 对应的开仓链"));
    }

    let entry = entry.expect("entry checked above");
    if entry == 0 {
        return Err(anyhow!("`--entry` 从 1 开始计数"));
    }

    entries
        .iter()
        .find(|item| item.entry == entry)
        .map(Some)
        .ok_or_else(|| anyhow!("未找到第 {entry} 笔开仓链"))
}

fn build_open_decision_detail(
    events: &[AuditEvent],
    markets: &[MarketConfig],
    selected_entry: &OpenDecisionListItem,
) -> Result<OpenDecisionDetailReport> {
    let market = markets
        .iter()
        .find(|market| market.symbol.0 == selected_entry.symbol)
        .ok_or_else(|| anyhow!("未找到市场 `{}` 的配置", selected_entry.symbol))?;
    let signal = events.iter().find_map(|event| {
        (event.correlation_id.as_deref() == Some(selected_entry.correlation_id.as_str()))
            .then_some(event)
            .and_then(signal_event)
    });
    let signal_id = signal.map(|item| item.candidate.candidate_id.as_str());
    let gate_chain = events
        .iter()
        .filter(|event| {
            matches!(event.payload, AuditEventPayload::GateDecision(_))
                && signal_id.is_some_and(|id| event.signal_id.as_deref() == Some(id))
        })
        .map(event_view)
        .collect::<Vec<_>>();

    let latest_market_snapshot = events.iter().rev().find(|event| {
        event.symbol.as_deref() == Some(selected_entry.symbol.as_str())
            && matches!(event.payload, AuditEventPayload::MarketMark(_))
    });
    let latest_market_snapshot =
        latest_market_snapshot.and_then(|event| latest_open_decision_market_view(event, market));

    let mut plans = Vec::new();
    let mut executions = Vec::new();
    let mut stage_by_plan_id = HashMap::new();
    for event in events.iter().filter(|event| {
        event.correlation_id.as_deref() == Some(selected_entry.correlation_id.as_str())
            && matches!(event.kind, polyalpha_audit::AuditEventKind::Execution)
    }) {
        let Some(execution) = execution_event(event) else {
            continue;
        };
        match execution {
            ExecutionEvent::TradePlanCreated { plan } => {
                let stage = open_plan_stage_label(plan).to_owned();
                stage_by_plan_id.insert(plan.plan_id.clone(), stage.clone());
                plans.push(open_decision_plan_view(event, plan, signal));
            }
            ExecutionEvent::ExecutionResultRecorded { result } => {
                let stage = stage_by_plan_id
                    .get(&result.plan_id)
                    .cloned()
                    .unwrap_or_else(|| "执行结果".to_owned());
                executions.push(open_decision_execution_view(event, &stage, result));
            }
            _ => {}
        }
    }

    Ok(OpenDecisionDetailReport {
        summary: empty_summary_stub(),
        entry: selected_entry.clone(),
        market: open_decision_market_view(market),
        signal: signal.map(|signal| open_decision_signal_view(signal, market)),
        latest_market_snapshot,
        gate_chain,
        plans,
        executions,
    })
}

fn open_decision_market_view(market: &MarketConfig) -> OpenDecisionMarketView {
    let rule = market.resolved_market_rule();
    OpenDecisionMarketView {
        question: market.market_question.clone(),
        rule_kind: rule
            .as_ref()
            .map(|rule| open_rule_kind_label(rule.kind).to_owned())
            .unwrap_or_else(|| "unknown".to_owned()),
        lower_strike: rule
            .as_ref()
            .and_then(|rule| rule.lower_strike)
            .map(|price| format_decimal(price.0)),
        upper_strike: rule
            .as_ref()
            .and_then(|rule| rule.upper_strike)
            .map(|price| format_decimal(price.0)),
        settlement_timestamp_ms: market.settlement_timestamp.saturating_mul(1000),
        settlement_time: format_timestamp_ms(market.settlement_timestamp.saturating_mul(1000)),
    }
}

fn open_decision_signal_view(
    signal: &SignalEmittedEvent,
    market: &MarketConfig,
) -> OpenDecisionSignalView {
    let observed = signal.observed.as_ref();
    let cex_reference_price = observed.and_then(|item| item.cex_mid);
    let minutes_to_expiry = minutes_to_expiry(signal.candidate.timestamp_ms, market);
    let implied_sigma_annualized = implied_sigma_annualized(
        market.resolved_market_rule().as_ref(),
        signal.candidate.token_side,
        cex_reference_price,
        signal.candidate.fair_value,
        minutes_to_expiry,
    );
    OpenDecisionSignalView {
        signal_id: signal.candidate.candidate_id.clone(),
        time: format_timestamp_ms(signal.candidate.timestamp_ms),
        token_side: format_token_side_label(signal.candidate.token_side).to_owned(),
        direction: signal.candidate.direction.clone(),
        candidate_price: observed_candidate_price(signal),
        fair_value: signal.candidate.fair_value,
        raw_mispricing: signal.candidate.raw_mispricing,
        delta_estimate: signal.candidate.delta_estimate,
        z_score: signal.candidate.z_score,
        cex_reference_price,
        minutes_to_expiry,
        raw_sigma_annualized: minute_sigma_to_annualized(signal.candidate.raw_sigma),
        effective_sigma_annualized: minute_sigma_to_annualized(signal.candidate.effective_sigma),
        sigma_source: signal.candidate.sigma_source.clone(),
        returns_window_len: signal.candidate.returns_window_len,
        implied_sigma_annualized,
        evaluable_status: observed
            .map(|item| item.evaluable_status.label_zh().to_owned())
            .unwrap_or_else(|| "无".to_owned()),
        async_classification: observed
            .map(|item| item.async_classification.label_zh().to_owned())
            .unwrap_or_else(|| "无".to_owned()),
        poly_quote_age_ms: observed.and_then(|item| item.poly_quote_age_ms),
        cex_quote_age_ms: observed.and_then(|item| item.cex_quote_age_ms),
        cross_leg_skew_ms: observed.and_then(|item| item.cross_leg_skew_ms),
    }
}

fn latest_open_decision_market_view(
    event: &AuditEvent,
    market: &MarketConfig,
) -> Option<OpenDecisionLatestMarketView> {
    let AuditEventPayload::MarketMark(mark) = &event.payload else {
        return None;
    };
    let token_side = mark
        .market
        .active_token_side
        .as_deref()
        .and_then(parse_token_side);
    let implied_sigma_annualized = token_side.and_then(|token_side| {
        implied_sigma_annualized(
            market.resolved_market_rule().as_ref(),
            token_side,
            mark.market.cex_price,
            mark.market.fair_value.unwrap_or_default(),
            mark.market.minutes_to_expiry,
        )
    });

    Some(OpenDecisionLatestMarketView {
        seq: event.seq,
        tick_index: mark.tick_index,
        time: format_timestamp_ms(event.timestamp_ms),
        active_token_side: mark.market.active_token_side.clone(),
        poly_price: mark.market.poly_price,
        cex_price: mark.market.cex_price,
        fair_value: mark.market.fair_value,
        minutes_to_expiry: mark.market.minutes_to_expiry,
        raw_sigma_annualized: minute_sigma_to_annualized(mark.market.raw_sigma),
        effective_sigma_annualized: minute_sigma_to_annualized(mark.market.effective_sigma),
        sigma_source: mark.market.sigma_source.clone(),
        returns_window_len: mark.market.returns_window_len,
        implied_sigma_annualized,
        evaluable_status: mark.market.evaluable_status.label_zh().to_owned(),
        async_classification: mark.market.async_classification.label_zh().to_owned(),
    })
}

fn open_decision_plan_view(
    event: &AuditEvent,
    plan: &TradePlan,
    signal: Option<&SignalEmittedEvent>,
) -> OpenDecisionPlanView {
    let signal_price = signal.and_then(observed_candidate_price);
    OpenDecisionPlanView {
        seq: event.seq,
        time: format_timestamp_ms(event.timestamp_ms),
        stage: open_plan_stage_label(plan).to_owned(),
        plan_id: plan.plan_id.clone(),
        parent_plan_id: plan.parent_plan_id.clone(),
        poly_side: format_order_side_label(plan.poly_side).to_owned(),
        poly_token_side: format_token_side_label(plan.poly_token_side).to_owned(),
        poly_planned_shares: format_decimal(plan.poly_planned_shares.0),
        poly_executable_price: format_decimal(plan.poly_executable_price.0),
        cex_side: format_order_side_label(plan.cex_side).to_owned(),
        cex_planned_qty: format_decimal(plan.cex_planned_qty.0),
        cex_executable_price: format_decimal(plan.cex_executable_price.0),
        planned_edge_usd: format_decimal(plan.planned_edge_usd.0),
        instant_open_loss_usd: format_decimal(instant_open_loss_usd(plan)),
        poly_avg_vs_signal_price_bps: poly_avg_vs_signal_price_bps(plan, signal_price),
        post_rounding_residual_delta: plan.post_rounding_residual_delta,
        shock_loss_up_2pct: format_decimal(plan.shock_loss_up_2pct.0),
        max_residual_delta: plan.max_residual_delta,
        max_shock_loss_usd: format_decimal(plan.max_shock_loss_usd.0),
    }
}

fn open_decision_execution_view(
    event: &AuditEvent,
    stage: &str,
    result: &polyalpha_core::ExecutionResult,
) -> OpenDecisionExecutionView {
    OpenDecisionExecutionView {
        seq: event.seq,
        time: format_timestamp_ms(event.timestamp_ms),
        stage: stage.to_owned(),
        plan_id: result.plan_id.clone(),
        status: result.status.clone(),
        realized_edge_usd: format_decimal(result.realized_edge_usd.0),
        actual_residual_delta: result.actual_residual_delta,
        recovery_required: result.recovery_required,
        poly_filled_qty: aggregate_filled_qty(&result.poly_order_ledger),
        cex_filled_qty: aggregate_filled_qty(&result.cex_order_ledger),
    }
}

fn empty_summary_stub() -> AuditSessionSummary {
    AuditSessionSummary {
        session_id: String::new(),
        env: String::new(),
        mode: Default::default(),
        status: Default::default(),
        started_at_ms: 0,
        ended_at_ms: None,
        updated_at_ms: 0,
        market_count: 0,
        markets: Vec::new(),
        counters: Default::default(),
        latest_tick_index: 0,
        latest_checkpoint_ms: None,
        latest_equity: None,
        latest_total_pnl_usd: None,
        latest_today_pnl_usd: None,
        latest_total_exposure_usd: None,
        rejection_reasons: HashMap::new(),
        skip_reasons: HashMap::new(),
        evaluable_status_counts: HashMap::new(),
        async_classification_counts: HashMap::new(),
        pulse_session_summaries: Vec::new(),
    }
}

fn signal_event(event: &AuditEvent) -> Option<&SignalEmittedEvent> {
    match &event.payload {
        AuditEventPayload::SignalEmitted(signal) => Some(signal),
        _ => None,
    }
}

fn observed_candidate_price(signal: &SignalEmittedEvent) -> Option<f64> {
    signal
        .observed
        .as_ref()
        .and_then(|observed| match signal.candidate.token_side {
            TokenSide::Yes => observed.poly_yes_mid,
            TokenSide::No => observed.poly_no_mid,
        })
}

fn minutes_to_expiry(timestamp_ms: u64, market: &MarketConfig) -> Option<f64> {
    let settlement_ms = market.settlement_timestamp.saturating_mul(1000);
    (settlement_ms > timestamp_ms)
        .then_some((settlement_ms.saturating_sub(timestamp_ms) as f64) / 60_000.0)
}

fn open_rule_kind_label(kind: MarketRuleKind) -> &'static str {
    match kind {
        MarketRuleKind::Above => "高于",
        MarketRuleKind::Below => "低于",
        MarketRuleKind::Between => "区间内",
    }
}

fn pricing_state_label(sigma_source: Option<&str>, returns_window_len: usize) -> String {
    match sigma_source {
        Some(source) if source.eq_ignore_ascii_case("realized") => {
            format!("已切换实时波动（样本 {} 笔）", returns_window_len)
        }
        _ if returns_window_len > 0 => format!(
            "参考价预热中（已积累 {} 笔，暂用默认波动）",
            returns_window_len
        ),
        _ => "参考价预热中（暂用默认波动）".to_owned(),
    }
}

fn open_signal_direction_label(direction: &str) -> &'static str {
    match direction {
        "long" => "开仓机会",
        "short" => "反向机会",
        _ => "候选机会",
    }
}

fn open_plan_stage_label(plan: &TradePlan) -> &'static str {
    match plan.intent_type.as_str() {
        "open_position" => "开仓计划",
        "delta_rebalance" if plan.parent_plan_id.is_some() => "开仓后补对冲",
        "delta_rebalance" => "补 CEX 对冲",
        _ => "执行计划",
    }
}

fn parse_token_side(value: &str) -> Option<TokenSide> {
    match value.trim().to_ascii_lowercase().as_str() {
        "yes" => Some(TokenSide::Yes),
        "no" => Some(TokenSide::No),
        _ => None,
    }
}

fn instant_open_loss_usd(plan: &TradePlan) -> Decimal {
    plan.poly_friction_cost_usd.0
        + plan.poly_fee_usd.0
        + plan.cex_friction_cost_usd.0
        + plan.cex_fee_usd.0
}

fn poly_avg_vs_signal_price_bps(plan: &TradePlan, signal_price: Option<f64>) -> Option<f64> {
    let signal_price = signal_price?;
    if signal_price <= 0.0 {
        return None;
    }
    let executable_price = plan.poly_executable_price.0.to_f64()?;
    Some((executable_price - signal_price) / signal_price * 10_000.0)
}

fn aggregate_filled_qty(entries: &[polyalpha_core::OrderLedgerEntry]) -> String {
    if entries.is_empty() {
        return "-".to_owned();
    }
    let total = entries.iter().fold(Decimal::ZERO, |acc, entry| {
        acc + entry.filled_qty.parse::<Decimal>().unwrap_or(Decimal::ZERO)
    });
    format_decimal(total)
}

const SQRT_TWO: f64 = std::f64::consts::SQRT_2;
const TRADING_DAYS_PER_YEAR: f64 = 252.0;
const MINUTES_PER_DAY: f64 = 1440.0;

fn implied_sigma_annualized(
    rule: Option<&MarketRule>,
    token_side: TokenSide,
    spot_price: Option<f64>,
    fair_value: f64,
    minutes_to_expiry: Option<f64>,
) -> Option<f64> {
    let rule = rule?;
    let spot_price = spot_price?;
    let minutes_to_expiry = minutes_to_expiry?;
    if !(0.0..=1.0).contains(&fair_value) || minutes_to_expiry <= 1.0 || spot_price <= 0.0 {
        return None;
    }

    let mut best_sigma = None;
    let mut best_error = f64::MAX;
    let mut sigma = 0.01;
    while sigma <= 3.0 {
        let minute_sigma = annualized_to_minute_sigma(sigma);
        let predicted = token_fair_value(
            rule,
            token_side,
            spot_price,
            minute_sigma,
            minutes_to_expiry,
        );
        let error = (predicted - fair_value).abs();
        if error < best_error {
            best_error = error;
            best_sigma = Some(sigma);
        }
        sigma += 0.001;
    }

    best_sigma
}

fn annualized_to_minute_sigma(value: f64) -> f64 {
    value / TRADING_DAYS_PER_YEAR.sqrt() / MINUTES_PER_DAY.sqrt()
}

fn minute_sigma_to_annualized(value: Option<f64>) -> Option<f64> {
    value.map(|sigma| sigma * TRADING_DAYS_PER_YEAR.sqrt() * MINUTES_PER_DAY.sqrt())
}

fn token_fair_value(
    rule: &MarketRule,
    token_side: TokenSide,
    spot_price: f64,
    minute_sigma: f64,
    minutes_to_expiry: f64,
) -> f64 {
    let yes_probability =
        yes_probability_from_rule(rule, spot_price, minute_sigma, minutes_to_expiry);
    match token_side {
        TokenSide::Yes => yes_probability.clamp(0.0, 1.0),
        TokenSide::No => (1.0 - yes_probability).clamp(0.0, 1.0),
    }
}

fn yes_probability_from_rule(
    rule: &MarketRule,
    spot_price: f64,
    minute_sigma: f64,
    minutes_to_expiry: f64,
) -> f64 {
    if minute_sigma <= 1e-6 || minutes_to_expiry <= 1.0 {
        return realized_yes_payout(rule, spot_price.max(1e-9));
    }

    let variance = (minute_sigma * minute_sigma * minutes_to_expiry).max(1e-12);
    let stdev = variance.sqrt();
    let mu = spot_price.max(1e-9).ln() - 0.5 * variance;
    let above_probability = |strike: f64| -> f64 {
        let z_value = ((strike.max(1e-9)).ln() - mu) / stdev;
        1.0 - normal_cdf(z_value)
    };

    match rule.kind {
        MarketRuleKind::Above => above_probability(
            rule.lower_strike
                .map(|price| price.0.to_f64().unwrap_or_default())
                .unwrap_or_default(),
        ),
        MarketRuleKind::Below => {
            1.0 - above_probability(
                rule.upper_strike
                    .map(|price| price.0.to_f64().unwrap_or_default())
                    .unwrap_or_default(),
            )
        }
        MarketRuleKind::Between => {
            let lower = rule
                .lower_strike
                .map(|price| price.0.to_f64().unwrap_or_default())
                .unwrap_or_default();
            let upper = rule
                .upper_strike
                .map(|price| price.0.to_f64().unwrap_or_default())
                .unwrap_or_default();
            (above_probability(lower) - above_probability(upper)).clamp(0.0, 1.0)
        }
    }
}

fn realized_yes_payout(rule: &MarketRule, terminal_price: f64) -> f64 {
    match rule.kind {
        MarketRuleKind::Above => {
            if terminal_price
                > rule
                    .lower_strike
                    .map(|price| price.0.to_f64().unwrap_or_default())
                    .unwrap_or_default()
            {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Below => {
            if terminal_price
                < rule
                    .upper_strike
                    .map(|price| price.0.to_f64().unwrap_or_default())
                    .unwrap_or_default()
            {
                1.0
            } else {
                0.0
            }
        }
        MarketRuleKind::Between => {
            let lower = rule
                .lower_strike
                .map(|price| price.0.to_f64().unwrap_or_default())
                .unwrap_or_default();
            let upper = rule
                .upper_strike
                .map(|price| price.0.to_f64().unwrap_or_default())
                .unwrap_or_default();
            if terminal_price >= lower && terminal_price < upper {
                1.0
            } else {
                0.0
            }
        }
    }
}

fn normal_cdf(value: f64) -> f64 {
    0.5 * (1.0 + erf(value / SQRT_TWO))
}

fn pre_close_snapshot_view(event: &AuditEvent) -> Option<TradeTimelinePreCloseView> {
    let AuditEventPayload::PositionMark(mark) = &event.payload else {
        return None;
    };

    Some(TradeTimelinePreCloseView {
        seq: event.seq,
        timestamp_ms: event.timestamp_ms,
        time: format_timestamp_ms(event.timestamp_ms),
        tick_index: mark.tick_index,
        poly_side: mark.position.poly_side.clone(),
        poly_quantity: format_f64_compact(mark.position.poly_quantity),
        poly_entry_price: format_f64_compact(mark.position.poly_entry_price),
        poly_current_price: format_f64_compact(mark.position.poly_current_price),
        cex_exchange: mark.position.cex_exchange.clone(),
        cex_side: mark.position.cex_side.clone(),
        cex_quantity: format_f64_compact(mark.position.cex_quantity),
        cex_entry_price: format_f64_compact(mark.position.cex_entry_price),
        cex_current_price: format_f64_compact(mark.position.cex_current_price),
        delta: format_f64_compact(mark.position.delta),
        total_pnl_usd: format_f64_compact(mark.position.total_pnl_usd),
    })
}

fn plan_timeline_row(
    event: &AuditEvent,
    plan: &TradePlan,
    stage: &str,
    is_recovery: bool,
) -> TradeTimelineRow {
    let (action, quantity, price, note) = if is_recovery {
        (
            format!("Poly {}", format_order_side_label(plan.poly_side)),
            format_decimal(plan.poly_requested_shares.0),
            format_decimal(plan.poly_executable_price.0),
            recovery_plan_note(plan),
        )
    } else if plan.cex_planned_qty.0 != Decimal::ZERO {
        (
            format!("CEX {}", format_order_side_label(plan.cex_side)),
            format_decimal(plan.cex_planned_qty.0),
            format_decimal(plan.cex_executable_price.0),
            child_rebalance_note(plan),
        )
    } else {
        (
            format!(
                "Poly {} {}",
                format_token_side_label(plan.poly_token_side),
                format_order_side_label(plan.poly_side)
            ),
            format_decimal(plan.poly_requested_shares.0),
            format_decimal(plan.poly_executable_price.0),
            "-".to_owned(),
        )
    };

    TradeTimelineRow {
        seq: event.seq,
        timestamp_ms: event.timestamp_ms,
        time: format_timestamp_ms(event.timestamp_ms),
        stage: stage.to_owned(),
        action,
        quantity,
        price,
        edge_usd: format_decimal(plan.planned_edge_usd.0),
        residual_delta: format_f64_compact(plan.post_rounding_residual_delta),
        note,
    }
}

fn fill_timeline_row(event: &AuditEvent, fill: &polyalpha_core::Fill) -> TradeTimelineRow {
    TradeTimelineRow {
        seq: event.seq,
        timestamp_ms: event.timestamp_ms,
        time: format_timestamp_ms(event.timestamp_ms),
        stage: "实际成交".to_owned(),
        action: format!(
            "{} {}",
            format_exchange_label(fill.exchange),
            format_order_side_label(fill.side)
        ),
        quantity: quantity_to_string(fill.quantity),
        price: format_decimal(fill.price.0),
        edge_usd: "-".to_owned(),
        residual_delta: "-".to_owned(),
        note: "-".to_owned(),
    }
}

fn result_timeline_row(
    event: &AuditEvent,
    stage: &str,
    result: &polyalpha_core::ExecutionResult,
) -> TradeTimelineRow {
    let (quantity, price) = result_primary_fill_view(result);
    let action = if result.recovery_required {
        "已成交，需处理剩余敞口"
    } else {
        "已成交"
    };

    TradeTimelineRow {
        seq: event.seq,
        timestamp_ms: event.timestamp_ms,
        time: format_timestamp_ms(event.timestamp_ms),
        stage: stage.to_owned(),
        action: action.to_owned(),
        quantity,
        price,
        edge_usd: format_decimal(result.realized_edge_usd.0),
        residual_delta: format_f64_compact(result.actual_residual_delta),
        note: "-".to_owned(),
    }
}

fn result_primary_fill_view(result: &polyalpha_core::ExecutionResult) -> (String, String) {
    if let Some(entry) = result.cex_order_ledger.first() {
        return (entry.filled_qty.clone(), entry.avg_price.clone());
    }
    if let Some(entry) = result.poly_order_ledger.first() {
        return (entry.filled_qty.clone(), entry.avg_price.clone());
    }
    ("-".to_owned(), "-".to_owned())
}

fn recovery_plan_note(plan: &TradePlan) -> String {
    let mut parts = Vec::new();
    if let Some(reason) = plan.recovery_decision_reason {
        parts.push(format!("原因: {}", recovery_reason_label(reason)));
    }
    if plan.cex_planned_qty.0 != Decimal::ZERO {
        parts.push(format!(
            "后续 CEX {} {} @ {}",
            format_order_side_label(plan.cex_side),
            format_decimal(plan.cex_planned_qty.0),
            format_decimal(plan.cex_executable_price.0)
        ));
    }
    if parts.is_empty() {
        "-".to_owned()
    } else {
        parts.join("；")
    }
}

fn child_rebalance_note(plan: &TradePlan) -> String {
    if plan.parent_plan_id.is_some() && plan.intent_type == "delta_rebalance" {
        "根据实际成交回补对冲".to_owned()
    } else {
        "-".to_owned()
    }
}

fn execution_event(event: &AuditEvent) -> Option<&ExecutionEvent> {
    match &event.payload {
        AuditEventPayload::Execution(execution) => Some(&execution.event),
        _ => None,
    }
}

fn trade_plan_stage_label(plan: &TradePlan) -> &'static str {
    match plan.intent_type.as_str() {
        "delta_rebalance" if plan.parent_plan_id.is_some() => "按实际成交回补对冲",
        "delta_rebalance" => "补 CEX 对冲",
        "close_position" => "发起平仓",
        "force_exit" => "强制退出",
        "residual_recovery" => "处理剩余敞口",
        _ => "执行计划",
    }
}

fn recovery_reason_label(reason: RecoveryDecisionReason) -> &'static str {
    match reason {
        RecoveryDecisionReason::CexTopUpCheaper => "补 CEX 更便宜",
        RecoveryDecisionReason::PolyFlattenCheaper => "平 Poly 更便宜",
        RecoveryDecisionReason::CexTopUpFaster => "补 CEX 更快",
        RecoveryDecisionReason::PolyFlattenOnlySafeRoute => "仅剩 Poly 平仓路径安全",
        RecoveryDecisionReason::GhostOrderGuardBlocksFlatten => "挂单守卫阻止 Poly 平仓",
        RecoveryDecisionReason::MarketPhaseBlocksPoly => "市场阶段不允许 Poly 平仓",
        RecoveryDecisionReason::CexDepthInsufficient => "CEX 深度不足",
        RecoveryDecisionReason::PolyDepthInsufficient => "Poly 深度不足",
        RecoveryDecisionReason::ForceExitRequired => "必须强制退出",
    }
}

fn format_exchange_label(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Polymarket => "Polymarket",
        Exchange::Binance => "Binance",
        Exchange::Deribit => "Deribit (unsupported)",
        Exchange::Okx => "OKX",
    }
}

fn format_order_side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "买入",
        OrderSide::Sell => "卖出",
    }
}

fn format_token_side_label(token_side: TokenSide) -> &'static str {
    match token_side {
        TokenSide::Yes => "YES",
        TokenSide::No => "NO",
    }
}

fn quantity_to_string(quantity: VenueQuantity) -> String {
    match quantity {
        VenueQuantity::PolyShares(shares) => format_decimal(shares.0),
        VenueQuantity::CexBaseQty(qty) => format_decimal(qty.0),
    }
}

fn format_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn format_hit_rate(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "-".to_owned())
}

fn parse_decimal_or_zero(value: &str) -> Decimal {
    value.parse::<Decimal>().unwrap_or(Decimal::ZERO)
}

fn format_signed_usd(value: f64) -> String {
    if value >= 0.0 {
        format!("+${value:.2}")
    } else {
        format!("-${:.2}", value.abs())
    }
}

fn format_f64_compact(value: f64) -> String {
    let value = if value.abs() < 1e-12 { 0.0 } else { value };
    let precision = if value.abs() >= 1.0 { 12 } else { 15 };
    let mut text = format!("{value:.precision$}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn print_trade_timeline_list(report: &TradeTimelineListReport) {
    println!("审计复盘: {}", report.summary.session_id);
    println!("环境: {}", report.summary.env);
    println!("状态: {}", report.summary.status.label_zh());
    println!(
        "运行区间: {} -> {}",
        format_timestamp_ms(report.summary.started_at_ms),
        report
            .summary
            .ended_at_ms
            .map(format_timestamp_ms)
            .unwrap_or_else(|| "仍在运行".to_owned())
    );
    println!("已平仓交易: {}", report.trades.len());
    if let Some(symbol) = report.symbol_filter.as_deref() {
        println!("市场过滤: {symbol}");
    }
    println!();
    println!("已平仓列表");

    if report.trades.is_empty() {
        println!("（无）");
        return;
    }

    for trade in &report.trades {
        println!("{}. {}  {}", trade.trade, trade.time, trade.symbol);
        println!("   已实现盈亏 {} USD", trade.realized_pnl_usd);
        println!("   平仓路径 {}", trade.path);
        println!("   相关ID {}", trade.correlation_id);
        println!();
    }

    println!("提示:");
    println!("- 用 `--trade <编号>` 查看单笔完整时间线");
    println!("- 或用 `--correlation-id <相关ID>` 直接定位单笔平仓");
}

fn print_trade_timeline_detail(report: &TradeTimelineDetailReport) {
    println!("交易复盘: {}", report.trade.symbol);
    println!("相关ID: {}", report.trade.correlation_id);
    println!("平仓结果: {} USD", report.trade.realized_pnl_usd);
    println!();
    println!("说明");
    if report.skew_detected {
        println!("- 当前为 mock 会话或干跑事件");
        println!("- 事件时间可能乱序");
        println!("- 下表按序号还原因果顺序，时间列用于辅助核对");
    } else {
        println!("- 下表按序号输出，时间列与因果顺序一致");
    }

    println!();
    println!("平仓前快照");
    if let Some(pre_close) = &report.pre_close {
        println!("- 时间 {}", pre_close.time);
        println!(
            "- Poly: {} {} 股，入场 {}，当前 {}",
            pre_close.poly_side,
            pre_close.poly_quantity,
            pre_close.poly_entry_price,
            pre_close.poly_current_price
        );
        println!(
            "- CEX: {} {} {}，入场 {}，当前 {}",
            pre_close.cex_exchange,
            pre_close.cex_side,
            pre_close.cex_quantity,
            pre_close.cex_entry_price,
            pre_close.cex_current_price
        );
        println!("- Delta: {}", pre_close.delta);
        println!("- 总浮盈亏: {} USD", pre_close.total_pnl_usd);
    } else {
        println!("- 无");
    }

    println!();
    println!("时间线");
    println!("序号 | 记录时间 | 阶段 | 动作 | 数量 | 价格 | 边际USD | 残余Delta | 说明");
    for row in &report.rows {
        println!(
            "{} | {} | {} | {} | {} | {} | {} | {} | {}",
            row.seq,
            row.time,
            row.stage,
            row.action,
            row.quantity,
            row.price,
            row.edge_usd,
            row.residual_delta,
            if row.note.is_empty() {
                "-"
            } else {
                row.note.as_str()
            }
        );
    }

    if let Some(attribution) = &report.attribution {
        println!();
        println!("亏损归因");
        if let Some(pre_close_mark_pnl_usd) = attribution.pre_close_mark_pnl_usd.as_deref() {
            println!("- 平仓前标记浮盈亏: {} USD", pre_close_mark_pnl_usd);
        }
        println!("- 平仓链额外损失: {} USD", attribution.close_chain_loss_usd);
        println!(
            "- 平仓前浮亏兑现: {} USD",
            attribution.pre_close_floating_loss_realized_usd
        );
    }
}

fn print_open_decision_list(report: &OpenDecisionListReport) {
    println!("开仓决策复盘: {}", report.summary.session_id);
    if let Some(symbol) = report.symbol_filter.as_deref() {
        println!("市场过滤: {symbol}");
    }
    println!("已开仓机会: {}", report.entries.len());
    if report.entries.is_empty() {
        println!("  （无）");
        return;
    }
    for entry in &report.entries {
        println!(
            "  [{}] {} {} {} 价={} FV={:.6} edge={} corr={}",
            entry.entry,
            entry.time,
            entry.symbol,
            entry.token_side,
            format_option_f64(entry.signal_price),
            entry.fair_value,
            entry.planned_edge_usd,
            entry.correlation_id
        );
    }
}

fn print_open_decision_detail(report: &OpenDecisionDetailReport) {
    println!(
        "开仓决策详情: {} / [{}] {}",
        report.summary.session_id, report.entry.entry, report.entry.symbol
    );
    println!(
        "题型规则: {} / 下沿={} / 上沿={} / 结算时间={}",
        report.market.rule_kind,
        format_option_text(report.market.lower_strike.as_deref()),
        format_option_text(report.market.upper_strike.as_deref()),
        report.market.settlement_time
    );
    if let Some(question) = report.market.question.as_deref() {
        println!("题面: {question}");
    }
    if let Some(signal) = &report.signal {
        println!(
            "信号: 时间={} 动作={} 买入方向={} 市场价={} 模型参考价={:.6} 模型价差={:.6} 对冲系数={:.12} Z值={}",
            signal.time,
            open_signal_direction_label(signal.direction.as_str()),
            signal.token_side,
            format_option_f64(signal.candidate_price),
            signal.fair_value,
            signal.raw_mispricing,
            signal.delta_estimate,
            format_option_f64(signal.z_score)
        );
        println!(
            "信号观测: CEX参考价={} 距结算剩余={} 参考价状态={} 当前参考波动={} 题面反推波动={} 状态={} / {}",
            format_option_f64(signal.cex_reference_price),
            format_option_f64(signal.minutes_to_expiry),
            pricing_state_label(signal.sigma_source.as_deref(), signal.returns_window_len),
            format_option_f64(signal.effective_sigma_annualized),
            format_option_f64(signal.implied_sigma_annualized),
            signal.evaluable_status,
            signal.async_classification
        );
        println!(
            "报价龄: Poly={} CEX={} 时差={}",
            signal
                .poly_quote_age_ms
                .map(format_duration_ms)
                .unwrap_or_else(|| "无".to_owned()),
            signal
                .cex_quote_age_ms
                .map(format_duration_ms)
                .unwrap_or_else(|| "无".to_owned()),
            signal
                .cross_leg_skew_ms
                .map(format_duration_ms)
                .unwrap_or_else(|| "无".to_owned())
        );
    }
    if let Some(mark) = &report.latest_market_snapshot {
        println!(
            "最新市场快照: tick={} 买入方向={} Poly={} CEX={} 模型参考价={} 距结算剩余={} 参考价状态={} 当前参考波动={} 题面反推波动={} 状态={} / {}",
            mark.tick_index,
            format_option_text(mark.active_token_side.as_deref()),
            format_option_f64(mark.poly_price),
            format_option_f64(mark.cex_price),
            format_option_f64(mark.fair_value),
            format_option_f64(mark.minutes_to_expiry),
            pricing_state_label(mark.sigma_source.as_deref(), mark.returns_window_len),
            format_option_f64(mark.effective_sigma_annualized),
            format_option_f64(mark.implied_sigma_annualized),
            mark.evaluable_status,
            mark.async_classification
        );
    }
    println!("Gate 链:");
    if report.gate_chain.is_empty() {
        println!("  （无）");
    } else {
        for event in &report.gate_chain {
            println!(
                "  {} [{}] {}",
                event.time,
                event.kind,
                format_event_table_summary(event)
            );
        }
    }
    println!("计划链:");
    if report.plans.is_empty() {
        println!("  （无）");
    } else {
        for plan in &report.plans {
            println!(
                "  {} [{}] Poly {} {} @ {} / CEX {} {} @ {} / edge={} / instant_loss={} / residual={} / avg_vs_signal_bps={}",
                plan.time,
                plan.stage,
                plan.poly_side,
                plan.poly_planned_shares,
                plan.poly_executable_price,
                plan.cex_side,
                plan.cex_planned_qty,
                plan.cex_executable_price,
                plan.planned_edge_usd,
                plan.instant_open_loss_usd,
                format_f64_compact(plan.post_rounding_residual_delta),
                format_option_f64(plan.poly_avg_vs_signal_price_bps)
            );
        }
    }
    println!("执行结果:");
    if report.executions.is_empty() {
        println!("  （无）");
    } else {
        for execution in &report.executions {
            println!(
                "  {} [{}] status={} realized_edge={} residual={} recovery_required={} poly_fill={} cex_fill={}",
                execution.time,
                execution.stage,
                execution.status,
                execution.realized_edge_usd,
                format_f64_compact(execution.actual_residual_delta),
                execution.recovery_required,
                execution.poly_filled_qty,
                execution.cex_filled_qty
            );
        }
    }
}

fn run_events_report(
    env: &str,
    session_id: Option<&str>,
    filters: AuditEventQueryFiltersView,
    limit: usize,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let filtered = filter_audit_events(&events, &filters);

    let report = AuditEventQueryReport {
        summary,
        filters,
        events: filtered
            .iter()
            .copied()
            .rev()
            .take(limit.max(1))
            .map(event_view)
            .collect(),
    };

    match format {
        AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        AuditOutputFormat::Table => {
            println!("审计事件查询: {}", report.summary.session_id);
            println!(
                "过滤条件: symbol={} kind={} gate={} result={} reason={} query={} limit={}",
                format_option_text(report.filters.symbol.as_deref()),
                format_option_text(report.filters.kind.as_deref()),
                format_option_text(report.filters.gate.as_deref()),
                format_option_text(report.filters.result.as_deref()),
                format_option_text(report.filters.reason.as_deref()),
                format_option_text(report.filters.query.as_deref()),
                report.filters.limit
            );
            println!("命中事件: {}", report.events.len());
            if report.events.is_empty() {
                println!("  （无）");
            } else {
                for event in &report.events {
                    println!(
                        "  {} [{}] {}",
                        event.time,
                        event.kind,
                        format_event_table_summary(event)
                    );
                }
            }
        }
    }

    Ok(())
}

fn run_session_summary(
    env: &str,
    session_id: Option<&str>,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);

    match format {
        AuditOutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "summary": summary,
                    "warehouse_path": warehouse_path.display().to_string(),
                }))?
            );
        }
        AuditOutputFormat::Table => {
            println!("审计会话: {}", summary.session_id);
            println!("环境: {}", summary.env);
            println!("状态: {}", summary.status.label_zh());
            println!("模式: {:?}", summary.mode);
            println!("市场数: {}", summary.market_count);
            println!("开始时间: {}", format_timestamp_ms(summary.started_at_ms));
            println!(
                "结束时间: {}",
                summary
                    .ended_at_ms
                    .map(format_timestamp_ms)
                    .unwrap_or_else(|| "仍在运行".to_owned())
            );
            println!(
                "最近更新时间: {}",
                format_timestamp_ms(summary.updated_at_ms)
            );
            println!(
                "轮次/行情/信号/拒绝: {}/{}/{}/{}",
                summary.counters.ticks_processed,
                summary.counters.market_events,
                summary.counters.signals_seen,
                summary.counters.signals_rejected
            );
            println!(
                "可评估/执行拒绝/成交: {}/{}/{}",
                summary.counters.evaluable_passes,
                summary.counters.execution_rejected,
                summary.counters.fills
            );
            println!(
                "平仓/轮询错误: {}/{}",
                summary.counters.trades_closed, summary.counters.poll_errors
            );
            println!(
                "评估漏斗: 评估={} -> 可评估={} -> 信号={} -> 被拒={} -> 执行拒绝={}",
                summary.counters.evaluation_attempts,
                summary.counters.evaluable_passes,
                summary.counters.signals_seen,
                summary.counters.signals_rejected,
                summary.counters.execution_rejected
            );
            println!(
                "最新净值/总盈亏/今日盈亏/总敞口: {}/{}/{}/{}",
                format_option_f64(summary.latest_equity),
                format_option_f64(summary.latest_total_pnl_usd),
                format_option_f64(summary.latest_today_pnl_usd),
                format_option_f64(summary.latest_total_exposure_usd)
            );
            println!(
                "最近检查点: {}",
                summary
                    .latest_checkpoint_ms
                    .map(format_timestamp_ms)
                    .unwrap_or_else(|| "无".to_owned())
            );
            println!("审计仓库: {}", warehouse_path.display());
            if !summary.rejection_reasons.is_empty() {
                println!(
                    "信号拒绝原因: {}",
                    format_counter_map(&summary.rejection_reasons)
                );
            }
            if !summary.skip_reasons.is_empty() {
                println!(
                    "评估跳过原因: {}",
                    format_counter_map_with_label(&summary.skip_reasons, skip_reason_label)
                );
            }
            if !summary.evaluable_status_counts.is_empty() {
                println!(
                    "评估状态分布: {}",
                    format_counter_map_with_label(
                        &summary.evaluable_status_counts,
                        evaluable_status_label
                    )
                );
            }
            if !summary.async_classification_counts.is_empty() {
                println!(
                    "异步分类分布: {}",
                    format_counter_map_with_label(
                        &summary.async_classification_counts,
                        async_classification_label
                    )
                );
            }
        }
    }

    Ok(())
}

fn run_pulse_sessions_report(
    env: &str,
    session_id: Option<&str>,
    asset: Option<&str>,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let raw_opening_kpi = collect_pulse_raw_opening_kpi(&events, asset);

    let sessions = summary
        .pulse_session_summaries
        .iter()
        .filter(|row| asset.is_none_or(|filter| row.asset.eq_ignore_ascii_case(filter)))
        .cloned()
        .collect::<Vec<_>>();

    let report = PulseSessionsReport {
        summary,
        asset_filter: asset.map(str::to_owned),
        raw_opening_kpi,
        sessions,
    };

    match format {
        AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        AuditOutputFormat::Table => {
            println!("Pulse 会话列表: {}", report.summary.session_id);
            println!("环境: {}", report.summary.env);
            println!(
                "资产过滤: {}",
                format_option_text(report.asset_filter.as_deref())
            );
            println!(
                "Raw Opening KPI: attempts={} positive_fill={} effective_open={} ({:.2}%) active_effective_open={}",
                report.raw_opening_kpi.opening_attempts,
                report.raw_opening_kpi.positive_fill_count,
                report.raw_opening_kpi.effective_open_count,
                report.raw_opening_kpi.effective_open_rate * 100.0,
                report.raw_opening_kpi.still_active_effective_open_count
            );
            println!(
                "Raw Opening 峰值: max_fill_notional={} max_expected_open_pnl={}",
                format_option_text(
                    report
                        .raw_opening_kpi
                        .max_actual_fill_notional_usd
                        .as_deref()
                ),
                format_option_text(
                    report
                        .raw_opening_kpi
                        .max_expected_open_net_pnl_usd
                        .as_deref()
                )
            );
            if !report
                .raw_opening_kpi
                .closed_rejection_reason_counts
                .is_empty()
            {
                println!(
                    "Raw Opening 拒绝原因: {}",
                    format_counter_map(&report.raw_opening_kpi.closed_rejection_reason_counts)
                );
            }
            if !report.raw_opening_kpi.effective_open_examples.is_empty() {
                println!("Raw Opening 样本:");
                for example in &report.raw_opening_kpi.effective_open_examples {
                    println!(
                        "  {} 资产={} 状态={} fill_notional={} expected_open_pnl={} hedge_qty={} anchor_lag={}",
                        example.session_id,
                        example.asset,
                        example.state,
                        example.actual_fill_notional_usd,
                        example.expected_open_net_pnl_usd,
                        example.opening_allocated_hedge_qty,
                        example
                            .anchor_latency_delta_ms
                            .map(|value| format!("{value}ms"))
                            .unwrap_or_else(|| "-".to_owned())
                    );
                }
            }
            println!("已归档会话: {}", report.sessions.len());
            if report.sessions.is_empty() {
                println!("  （无）");
            } else {
                for row in &report.sessions {
                    println!(
                        "  {} 资产={} 状态={} fill={}/{} ({:.1}%) edge={} cand={} open={} timeout_loss={} hit={} pocket={} vacuum={} pnl={}",
                        row.pulse_session_id,
                        row.asset,
                        row.state,
                        row.actual_poly_filled_qty,
                        row.planned_poly_qty,
                        row.actual_poly_fill_ratio * 100.0,
                        row.net_edge_bps
                            .map(|value| format!("{value:.1}bps"))
                            .unwrap_or_else(|| "-".to_owned()),
                        format_option_text(row.candidate_expected_net_pnl_usd.as_deref()),
                        row.expected_open_net_pnl_usd,
                        format_option_text(row.timeout_loss_estimate_usd.as_deref()),
                        format_hit_rate(row.required_hit_rate),
                        row.reversion_pocket_ticks
                            .map(|value| format!("{value:.1}t"))
                            .unwrap_or_else(|| "-".to_owned()),
                        format_option_text(row.vacuum_ratio.as_deref()),
                        row.realized_pnl_usd
                            .map(format_signed_usd)
                            .unwrap_or_else(|| "-".to_owned())
                    );
                }
            }
        }
    }

    Ok(())
}

fn collect_pulse_raw_opening_kpi(
    events: &[AuditEvent],
    asset_filter: Option<&str>,
) -> PulseRawOpeningKpi {
    let mut sessions = HashMap::<String, PulseRawOpeningSessionAggregate>::new();

    for event in events {
        let AuditEventPayload::PulseLifecycle(payload) = &event.payload else {
            continue;
        };
        if asset_filter.is_some_and(|filter| !payload.asset.eq_ignore_ascii_case(filter)) {
            continue;
        }

        let aggregate = sessions
            .entry(payload.session_id.clone())
            .or_insert_with(|| PulseRawOpeningSessionAggregate {
                asset: payload.asset.clone(),
                ..PulseRawOpeningSessionAggregate::default()
            });

        let actual_fill_notional_usd = parse_decimal_or_zero(&payload.actual_fill_notional_usd);
        let actual_poly_filled_qty = parse_decimal_or_zero(&payload.actual_poly_filled_qty);
        if actual_fill_notional_usd > Decimal::ZERO || actual_poly_filled_qty > Decimal::ZERO {
            aggregate.positive_fill = true;
        }
        if payload.effective_open || payload.opening_outcome == "effective_open" {
            aggregate.effective_open = true;
        }
        if event.seq >= aggregate.latest_seq {
            aggregate.asset = payload.asset.clone();
            aggregate.latest_seq = event.seq;
            aggregate.latest_state = payload.state.clone();
            aggregate.latest_actual_fill_notional_usd = actual_fill_notional_usd;
            aggregate.latest_expected_open_net_pnl_usd =
                parse_decimal_or_zero(&payload.expected_open_net_pnl_usd);
            aggregate.latest_opening_allocated_hedge_qty =
                payload.opening_allocated_hedge_qty.clone();
            aggregate.latest_anchor_latency_delta_ms = payload.anchor_latency_delta_ms;
            aggregate.latest_opening_rejection_reason = payload.opening_rejection_reason.clone();
        }
    }

    let opening_attempts = sessions.len();
    let positive_fill_count = sessions
        .values()
        .filter(|session| session.positive_fill)
        .count();
    let effective_open_count = sessions
        .values()
        .filter(|session| session.effective_open)
        .count();
    let still_active_effective_open_count = sessions
        .values()
        .filter(|session| session.effective_open && !session.latest_state.eq("closed"))
        .count();

    let mut closed_rejection_reason_counts = HashMap::new();
    for session in sessions
        .values()
        .filter(|session| !session.effective_open && session.latest_state == "closed")
    {
        if let Some(reason) = &session.latest_opening_rejection_reason {
            *closed_rejection_reason_counts
                .entry(reason.clone())
                .or_insert(0) += 1;
        }
    }

    let max_actual_fill_notional_usd = sessions
        .values()
        .map(|session| session.latest_actual_fill_notional_usd)
        .max()
        .map(format_decimal);
    let max_expected_open_net_pnl_usd = sessions
        .values()
        .map(|session| session.latest_expected_open_net_pnl_usd)
        .max()
        .map(format_decimal);

    let mut effective_open_examples = sessions
        .into_iter()
        .filter_map(|(session_id, session)| {
            session.effective_open.then_some(PulseRawOpeningExample {
                session_id,
                asset: session.asset,
                state: session.latest_state,
                actual_fill_notional_usd: format_decimal(session.latest_actual_fill_notional_usd),
                expected_open_net_pnl_usd: format_decimal(session.latest_expected_open_net_pnl_usd),
                opening_allocated_hedge_qty: session.latest_opening_allocated_hedge_qty,
                anchor_latency_delta_ms: session.latest_anchor_latency_delta_ms,
            })
        })
        .collect::<Vec<_>>();
    effective_open_examples.sort_by(|left, right| {
        parse_decimal_or_zero(&right.expected_open_net_pnl_usd)
            .cmp(&parse_decimal_or_zero(&left.expected_open_net_pnl_usd))
            .then_with(|| {
                parse_decimal_or_zero(&right.actual_fill_notional_usd)
                    .cmp(&parse_decimal_or_zero(&left.actual_fill_notional_usd))
            })
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    effective_open_examples.truncate(5);

    PulseRawOpeningKpi {
        opening_attempts,
        positive_fill_count,
        effective_open_count,
        effective_open_rate: if opening_attempts == 0 {
            0.0
        } else {
            effective_open_count as f64 / opening_attempts as f64
        },
        still_active_effective_open_count,
        closed_rejection_reason_counts,
        max_actual_fill_notional_usd,
        max_expected_open_net_pnl_usd,
        effective_open_examples,
    }
}

fn run_pulse_session_detail_report(
    env: &str,
    session_id: Option<&str>,
    pulse_session_id: &str,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;

    let session = summary
        .pulse_session_summaries
        .iter()
        .find(|row| row.pulse_session_id == pulse_session_id)
        .cloned()
        .ok_or_else(|| anyhow!("未找到 pulse session `{pulse_session_id}`"))?;

    let timeline = build_pulse_session_timeline(&events, pulse_session_id);
    let report = PulseSessionDetailAuditReport {
        summary,
        session,
        timeline,
    };

    match format {
        AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        AuditOutputFormat::Table => {
            println!("Pulse 会话详情: {}", report.session.pulse_session_id);
            println!("审计会话: {}", report.summary.session_id);
            println!("资产: {}", report.session.asset);
            println!("状态: {}", report.session.state);
            println!(
                "开/平时间: {} / {}",
                format_timestamp_ms(report.session.opened_at_ms),
                report
                    .session
                    .closed_at_ms
                    .map(format_timestamp_ms)
                    .unwrap_or_else(|| "仍在运行".to_owned())
            );
            println!(
                "Poly 成交: {}/{} ({:.1}%)",
                report.session.actual_poly_filled_qty,
                report.session.planned_poly_qty,
                report.session.actual_poly_fill_ratio * 100.0
            );
            println!(
                "信号: pulse={} entry={} target={} timeout={}",
                report
                    .session
                    .pulse_score_bps
                    .map(|value| format!("{value:.1}bps"))
                    .unwrap_or_else(|| "-".to_owned()),
                format_option_text(report.session.entry_price.as_deref()),
                format_option_text(report.session.target_exit_price.as_deref()),
                format_option_text(report.session.timeout_exit_price.as_deref())
            );
            println!(
                "会话EV: candidate={} open={} timeout_loss={} hit={} entry_notional={} pocket_ticks={} pocket_notional={} vacuum={}",
                format_option_text(report.session.candidate_expected_net_pnl_usd.as_deref()),
                report.session.expected_open_net_pnl_usd,
                format_option_text(report.session.timeout_loss_estimate_usd.as_deref()),
                format_hit_rate(report.session.required_hit_rate),
                format_option_text(report.session.entry_executable_notional_usd.as_deref()),
                report
                    .session
                    .reversion_pocket_ticks
                    .map(|value| format!("{value:.1}"))
                    .unwrap_or_else(|| "-".to_owned()),
                format_option_text(report.session.reversion_pocket_notional_usd.as_deref()),
                format_option_text(report.session.vacuum_ratio.as_deref())
            );
            println!(
                "对冲归因: session_delta={} allocated_hedge={}",
                report.session.session_target_delta_exposure,
                report.session.session_allocated_hedge_qty
            );
            println!(
                "审计指标: anchor_latency={} queue_distance={} order_age={}",
                report
                    .session
                    .anchor_latency_delta_ms
                    .map(|value| format!("{value}ms"))
                    .unwrap_or_else(|| "-".to_owned()),
                report
                    .session
                    .distance_to_mid_bps
                    .map(|value| format!("{value:.1}bps"))
                    .unwrap_or_else(|| "-".to_owned()),
                report
                    .session
                    .relative_order_age_ms
                    .map(|value| format!("{value}ms"))
                    .unwrap_or_else(|| "-".to_owned())
            );
            println!(
                "结果: edge={} pnl={}",
                report
                    .session
                    .net_edge_bps
                    .map(|value| format!("{value:.1}bps"))
                    .unwrap_or_else(|| "-".to_owned()),
                report
                    .session
                    .realized_pnl_usd
                    .map(format_signed_usd)
                    .unwrap_or_else(|| "-".to_owned())
            );
            let exit_slippage = match (
                report.session.target_exit_price.as_deref(),
                report.session.final_exit_price.as_deref(),
            ) {
                (Some(target), Some(final_price)) => {
                    Some(parse_decimal_or_zero(final_price) - parse_decimal_or_zero(target))
                }
                _ => None,
            };
            println!(
                "离场: path={} target={} final={} slippage={}",
                report
                    .session
                    .exit_path
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
                report
                    .session
                    .target_exit_price
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
                report
                    .session
                    .final_exit_price
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
                exit_slippage
                    .map(format_decimal)
                    .unwrap_or_else(|| "-".to_owned())
            );
            println!("时间线事件: {}", report.timeline.len());
            if report.timeline.is_empty() {
                println!("  （无）");
            } else {
                for item in &report.timeline {
                    println!(
                        "  #{} {} {} state={} {}{}",
                        item.seq,
                        item.time,
                        item.kind,
                        item.state,
                        item.summary,
                        item.instrument
                            .as_ref()
                            .map(|value| format!(" instrument={value}"))
                            .unwrap_or_default()
                    );
                }
            }
        }
    }

    Ok(())
}

fn build_pulse_session_timeline(
    events: &[AuditEvent],
    pulse_session_id: &str,
) -> Vec<PulseSessionTimelineItem> {
    let mut timeline = events
        .iter()
        .filter_map(|event| match &event.payload {
            AuditEventPayload::PulseLifecycle(payload)
                if payload.session_id == pulse_session_id =>
            {
                Some(PulseSessionTimelineItem {
                    seq: event.seq,
                    timestamp_ms: event.timestamp_ms,
                    time: format_timestamp_ms(event.timestamp_ms),
                    kind: event.kind.label_zh().to_owned(),
                    state: payload.state.clone(),
                    summary: event.summary.clone(),
                    instrument: None,
                })
            }
            AuditEventPayload::PulseBookTape(payload) if payload.session_id == pulse_session_id => {
                Some(PulseSessionTimelineItem {
                    seq: event.seq,
                    timestamp_ms: event.timestamp_ms,
                    time: format_timestamp_ms(event.timestamp_ms),
                    kind: event.kind.label_zh().to_owned(),
                    state: payload.state.clone(),
                    summary: format!(
                        "{} {} seq={}",
                        payload.symbol, payload.book.instrument, payload.book.sequence
                    ),
                    instrument: Some(payload.book.instrument.clone()),
                })
            }
            AuditEventPayload::PulseSessionSummary(payload)
                if payload.pulse_session_id == pulse_session_id =>
            {
                Some(PulseSessionTimelineItem {
                    seq: event.seq,
                    timestamp_ms: event.timestamp_ms,
                    time: format_timestamp_ms(event.timestamp_ms),
                    kind: event.kind.label_zh().to_owned(),
                    state: payload.state.clone(),
                    summary: event.summary.clone(),
                    instrument: None,
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    timeline.sort_by_key(|item| (item.timestamp_ms, item.seq));
    timeline
}

fn run_market_report(
    env: &str,
    session_id: Option<&str>,
    symbol: &str,
    limit: usize,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let filtered = filter_symbol_events(&events, symbol);
    let latest_market_mark = filtered
        .iter()
        .rev()
        .find_map(|event| match &event.payload {
            AuditEventPayload::MarketMark(mark) => Some(mark.clone()),
            _ => None,
        });
    let latest_position_mark = filtered
        .iter()
        .rev()
        .find_map(|event| match &event.payload {
            AuditEventPayload::PositionMark(mark) => Some(mark.clone()),
            _ => None,
        });

    let report = MarketAuditReport {
        summary,
        symbol: symbol.to_owned(),
        latest_market_mark,
        latest_position_mark,
        recent_events: filtered
            .iter()
            .copied()
            .rev()
            .take(limit.max(1))
            .map(event_view)
            .collect(),
    };

    match format {
        AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        AuditOutputFormat::Table => {
            println!(
                "审计市场复盘: {} / {}",
                report.summary.session_id, report.symbol
            );
            if let Some(mark) = &report.latest_market_mark {
                println!(
                    "最新市场快照: Z={} 基差={} Poly={} CEX={} 公允价={}",
                    format_option_f64(mark.market.z_score),
                    format_option_f64(mark.market.basis_pct),
                    format_option_f64(mark.market.poly_price),
                    format_option_f64(mark.market.cex_price),
                    format_option_f64(mark.market.fair_value)
                );
                println!(
                    "最近行情时间: Poly={} CEX={}",
                    mark.market
                        .poly_updated_at_ms
                        .map(format_timestamp_ms)
                        .unwrap_or_else(|| "无".to_owned()),
                    mark.market
                        .cex_updated_at_ms
                        .map(format_timestamp_ms)
                        .unwrap_or_else(|| "无".to_owned())
                );
                println!(
                    "报价龄/时差/状态: Poly={} CEX={} 时差={} 状态={} 异步={}",
                    mark.market
                        .poly_quote_age_ms
                        .map(format_duration_ms)
                        .unwrap_or_else(|| "无".to_owned()),
                    mark.market
                        .cex_quote_age_ms
                        .map(format_duration_ms)
                        .unwrap_or_else(|| "无".to_owned()),
                    mark.market
                        .cross_leg_skew_ms
                        .map(format_duration_ms)
                        .unwrap_or_else(|| "无".to_owned()),
                    mark.market.evaluable_status.label_zh(),
                    mark.market.async_classification.label_zh(),
                );
            } else {
                println!("最新市场快照: 无");
            }
            if let Some(position) = &report.latest_position_mark {
                println!(
                    "最新持仓快照: Poly={} @ {} / CEX={} @ {} / 总盈亏={} / Delta={} / 对冲比={}",
                    position.position.poly_side,
                    position.position.poly_current_price,
                    position.position.cex_side,
                    position.position.cex_current_price,
                    position.position.total_pnl_usd,
                    position.position.delta,
                    position.position.hedge_ratio
                );
            }
            println!("最近相关事件:");
            if report.recent_events.is_empty() {
                println!("  （无）");
            } else {
                for event in &report.recent_events {
                    println!(
                        "  {} [{}] {}",
                        event.time,
                        event.kind,
                        format_event_table_summary(event)
                    );
                }
            }
        }
    }

    Ok(())
}

fn run_position_explain(
    env: &str,
    session_id: Option<&str>,
    symbol: &str,
    limit: usize,
    format: AuditOutputFormat,
) -> Result<()> {
    let (settings, summary) = resolve_session(env, session_id)?;
    let _warehouse_path =
        sync_warehouse_best_effort(&settings.general.data_dir, &summary.session_id);
    let events = AuditReader::load_events(&settings.general.data_dir, &summary.session_id)?;
    let checkpoint = AuditReader::load_checkpoint(&settings.general.data_dir, &summary.session_id)?;
    let filtered = filter_symbol_events(&events, symbol);

    let latest_signal = filtered
        .iter()
        .rev()
        .find_map(|event| match &event.payload {
            AuditEventPayload::SignalEmitted(signal) => Some(signal.clone()),
            _ => None,
        });
    let latest_signal_id = latest_signal
        .as_ref()
        .map(|signal| signal.candidate.candidate_id.as_str());
    let gate_chain = filtered
        .iter()
        .copied()
        .filter(|event| {
            matches!(event.payload, AuditEventPayload::GateDecision(_))
                && latest_signal_id
                    .is_some_and(|signal_id| event.signal_id.as_deref() == Some(signal_id))
        })
        .map(event_view)
        .collect::<Vec<_>>();
    let executions = filtered
        .iter()
        .copied()
        .filter(|event| {
            matches!(event.payload, AuditEventPayload::Execution(_))
                && latest_signal_id
                    .map(|signal_id| event.signal_id.as_deref() == Some(signal_id))
                    .unwrap_or(true)
        })
        .rev()
        .take(limit.max(1))
        .map(event_view)
        .collect::<Vec<_>>();
    let latest_position_mark = filtered
        .iter()
        .rev()
        .find_map(|event| match &event.payload {
            AuditEventPayload::PositionMark(mark) => Some(mark.clone()),
            _ => None,
        });
    let latest_closed_trade = checkpoint.as_ref().and_then(|cp| {
        cp.monitor_state
            .recent_trades
            .iter()
            .find(|trade| symbol_matches(&trade.market, symbol))
            .cloned()
    });

    let report = PositionExplainReport {
        summary,
        symbol: symbol.to_owned(),
        latest_signal,
        gate_chain,
        executions,
        latest_position_mark,
        latest_closed_trade,
        recent_events: filtered
            .iter()
            .copied()
            .rev()
            .take(limit.max(1))
            .map(event_view)
            .collect(),
    };

    match format {
        AuditOutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        AuditOutputFormat::Table => {
            println!(
                "持仓解释: {} / {}",
                report.summary.session_id, report.symbol
            );
            if let Some(signal) = &report.latest_signal {
                println!(
                    "最近候选: {} {} @ {}",
                    signal.candidate.candidate_id,
                    candidate_direction_text(&signal.candidate.direction),
                    format_timestamp_ms(signal.candidate.timestamp_ms)
                );
                if let Some(observed) = &signal.observed {
                    println!(
                        "信号观测: Poly报价龄={} CEX报价龄={} 双腿时差={} 状态={} 异步={}",
                        observed
                            .poly_quote_age_ms
                            .map(format_duration_ms)
                            .unwrap_or_else(|| "无".to_owned()),
                        observed
                            .cex_quote_age_ms
                            .map(format_duration_ms)
                            .unwrap_or_else(|| "无".to_owned()),
                        observed
                            .cross_leg_skew_ms
                            .map(format_duration_ms)
                            .unwrap_or_else(|| "无".to_owned()),
                        observed.evaluable_status.label_zh(),
                        observed.async_classification.label_zh()
                    );
                }
            } else {
                println!("最近候选: 无");
            }
            if let Some(position) = &report.latest_position_mark {
                println!(
                    "最近持仓快照: Poly={} {}股 入场={} 当前={} | CEX={} 数量={} 入场={} 当前={} | 基差入场/当前={}/{} bps | Delta={} | 总盈亏={}",
                    position.position.poly_side,
                    position.position.poly_quantity,
                    position.position.poly_entry_price,
                    position.position.poly_current_price,
                    position.position.cex_side,
                    position.position.cex_quantity,
                    position.position.cex_entry_price,
                    position.position.cex_current_price,
                    position.position.basis_entry_bps,
                    position.position.basis_current_bps,
                    position.position.delta,
                    position.position.total_pnl_usd
                );
            } else {
                println!("最近持仓快照: 无");
            }
            if let Some(trade) = &report.latest_closed_trade {
                println!(
                    "最近已平仓: {} -> {}，已实现盈亏={}",
                    format_timestamp_ms(trade.opened_at_ms),
                    format_timestamp_ms(trade.closed_at_ms),
                    trade.realized_pnl_usd
                );
            }
            println!("闸门链路:");
            if report.gate_chain.is_empty() {
                println!("  （无）");
            } else {
                for event in &report.gate_chain {
                    println!(
                        "  {} [{}] {}",
                        event.time,
                        event.kind,
                        format_event_table_summary(event)
                    );
                }
            }
            println!("执行链路:");
            if report.executions.is_empty() {
                println!("  （无）");
            } else {
                for event in &report.executions {
                    println!(
                        "  {} [{}] {}",
                        event.time,
                        event.kind,
                        format_event_table_summary(event)
                    );
                }
            }
            println!("最近相关事件:");
            for event in &report.recent_events {
                println!(
                    "  {} [{}] {}",
                    event.time,
                    event.kind,
                    format_event_table_summary(event)
                );
            }
        }
    }

    Ok(())
}

fn resolve_session(env: &str, session_id: Option<&str>) -> Result<(Settings, AuditSessionSummary)> {
    let settings = Settings::load(env).with_context(|| format!("加载配置环境 `{env}` 失败"))?;
    let summary = match session_id {
        Some(session_id) => AuditReader::load_summary(&settings.general.data_dir, session_id)?,
        None => AuditReader::latest_session_summary(&settings.general.data_dir, Some(env))?
            .ok_or_else(|| anyhow!("未找到环境 `{env}` 的审计会话"))?,
    };
    Ok((settings, summary))
}

fn sync_warehouse_best_effort(data_dir: &str, session_id: &str) -> PathBuf {
    let warehouse_path = PathBuf::from(data_dir)
        .join("audit")
        .join("warehouse")
        .join("audit.duckdb");
    match AuditWarehouse::new(data_dir).and_then(|warehouse| {
        warehouse
            .sync_session(data_dir, session_id)
            .with_context(|| format!("同步审计会话 `{session_id}` 到仓库失败"))?;
        Ok(warehouse)
    }) {
        Ok(warehouse) => warehouse.db_path().to_path_buf(),
        Err(err) => {
            eprintln!("审计仓库暂不可写，已回退读取原始审计文件：{err}");
            warehouse_path
        }
    }
}

fn filter_symbol_events<'a>(events: &'a [AuditEvent], symbol: &str) -> Vec<&'a AuditEvent> {
    events
        .iter()
        .filter(|event| {
            event
                .symbol
                .as_deref()
                .is_some_and(|value| symbol_matches(value, symbol))
        })
        .collect()
}

fn filter_audit_events<'a>(
    events: &'a [AuditEvent],
    query: &AuditEventQueryFiltersView,
) -> Vec<&'a AuditEvent> {
    events
        .iter()
        .filter(|event| audit_event_matches(event, query))
        .collect()
}

fn audit_event_matches(event: &AuditEvent, query: &AuditEventQueryFiltersView) -> bool {
    if query.symbol.as_deref().is_some_and(|symbol| {
        !event
            .symbol
            .as_deref()
            .is_some_and(|value| symbol_matches(value, symbol))
    }) {
        return false;
    }
    if query.kind.as_deref().is_some_and(|kind| {
        !text_matches(event.kind.as_str(), kind) && !text_matches(event.kind.label_zh(), kind)
    }) {
        return false;
    }
    if query.gate.as_deref().is_some_and(|gate| {
        !event
            .gate
            .as_deref()
            .is_some_and(|value| text_matches(value, gate))
    }) {
        return false;
    }
    if query.result.as_deref().is_some_and(|result| {
        !event
            .result
            .as_deref()
            .is_some_and(|value| text_matches(value, result))
    }) {
        return false;
    }
    if query.reason.as_deref().is_some_and(|reason| {
        !event
            .reason
            .as_deref()
            .is_some_and(|value| text_matches(value, reason))
    }) {
        return false;
    }
    if query
        .query
        .as_deref()
        .is_some_and(|needle| !audit_event_contains(event, needle))
    {
        return false;
    }
    true
}

fn audit_event_contains(event: &AuditEvent, query: &str) -> bool {
    [
        Some(event.summary.as_str()),
        event.symbol.as_deref(),
        event.signal_id.as_deref(),
        event.correlation_id.as_deref(),
        event.gate.as_deref(),
        event.result.as_deref(),
        event.reason.as_deref(),
        Some(event.kind.as_str()),
        Some(event.kind.label_zh()),
    ]
    .into_iter()
    .flatten()
    .any(|value| text_matches(value, query))
}

fn symbol_matches(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn text_matches(value: &str, query: &str) -> bool {
    normalize_text(value).contains(&normalize_text(query))
}

fn normalize_text(value: &str) -> String {
    value.trim().to_lowercase()
}

fn event_view(event: &AuditEvent) -> AuditEventView {
    AuditEventView {
        timestamp_ms: event.timestamp_ms,
        time: format_timestamp_ms(event.timestamp_ms),
        kind: event.kind.label_zh().to_owned(),
        summary: event.summary.clone(),
        symbol: event.symbol.clone(),
        signal_id: event.signal_id.clone(),
        correlation_id: event.correlation_id.clone(),
        gate: event.gate.clone(),
        result: event.result.clone(),
        reason: event.reason.clone(),
    }
}

fn format_event_table_summary(event: &AuditEventView) -> String {
    let mut suffix = Vec::new();
    if let Some(gate) = event.gate.as_deref() {
        suffix.push(format!("闸门={gate}"));
    }
    if let Some(result) = event.result.as_deref() {
        suffix.push(format!("结果={result}"));
    }
    if let Some(reason) = event.reason.as_deref() {
        suffix.push(format!("原因={reason}"));
    }
    if suffix.is_empty() {
        event.summary.clone()
    } else {
        format!("{}（{}）", event.summary, suffix.join("，"))
    }
}

fn format_timestamp_ms(timestamp_ms: u64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms as i64)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn format_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| if value.abs() < 1e-9 { 0.0 } else { value })
        .map(|value| format!("{value:.6}"))
        .unwrap_or_else(|| "无".to_owned())
}

fn format_option_text(value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("无")
        .to_owned()
}

fn format_counter_map(values: &std::collections::HashMap<String, u64>) -> String {
    let mut items = values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    items.sort();
    items.join(", ")
}

fn format_counter_map_with_label(
    values: &std::collections::HashMap<String, u64>,
    label_fn: fn(&str) -> String,
) -> String {
    let mut items = values
        .iter()
        .map(|(key, value)| format!("{}={value}", label_fn(key)))
        .collect::<Vec<_>>();
    items.sort();
    items.join(", ")
}

fn format_duration_ms(value: u64) -> String {
    if value < 10_000 {
        format!("{:.1}s", value as f64 / 1000.0)
    } else if value < 60_000 {
        format!("{}s", value / 1000)
    } else if value < 3_600_000 {
        format!("{}m", value / 60_000)
    } else {
        format!("{}h", value / 3_600_000)
    }
}

fn skip_reason_label(reason: &str) -> String {
    match reason {
        "no_fresh_poly_sample" => "缺少新的 Polymarket 样本".to_owned(),
        "poly_price_stale" => "Polymarket 报价已超时".to_owned(),
        "cex_price_stale" => "交易所报价已超时".to_owned(),
        "data_misaligned" => "双腿时间未对齐".to_owned(),
        "connection_lost" => "行情连接异常".to_owned(),
        "no_cex_reference" => "缺少可用的交易所参考价".to_owned(),
        _ => reason.to_owned(),
    }
}

fn evaluable_status_label(status: &str) -> String {
    match status {
        "evaluable" => "可评估".to_owned(),
        "not_evaluable_no_poly" => "缺少新的 Polymarket 样本".to_owned(),
        "not_evaluable_no_cex" => "缺少可用的交易所参考价".to_owned(),
        "not_evaluable_misaligned" => "双腿时间未对齐".to_owned(),
        "connection_impaired" => "行情连接异常".to_owned(),
        "poly_transport_stale" => "Polymarket WS 已超时".to_owned(),
        "hedge_transport_stale" => "交易所 WS 已超时".to_owned(),
        "poly_quote_stale" => "Polymarket 报价已超时".to_owned(),
        "cex_quote_stale" => "交易所报价已超时".to_owned(),
        _ => status.to_owned(),
    }
}

fn async_classification_label(classification: &str) -> String {
    match classification {
        "balanced_fresh" => "双腿新鲜".to_owned(),
        "poly_lag_acceptable" => "慢腿可交易".to_owned(),
        "poly_lag_borderline" => "慢腿临界".to_owned(),
        "no_poly_sample" => "缺少新的 Polymarket 样本".to_owned(),
        "no_cex_reference" => "缺少可用的交易所参考价".to_owned(),
        "poly_transport_stale" => "Polymarket WS 已超时".to_owned(),
        "hedge_transport_stale" => "交易所 WS 已超时".to_owned(),
        "poly_quote_stale" => "Polymarket 报价已超时".to_owned(),
        "cex_quote_stale" => "交易所报价已超时".to_owned(),
        "misaligned" => "双腿时间未对齐".to_owned(),
        "connection_impaired" => "行情连接异常".to_owned(),
        _ => classification.to_owned(),
    }
}

fn candidate_direction_text(direction: &str) -> &str {
    match direction {
        "long" => "开仓机会",
        "short" => "反向机会",
        _ => direction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_audit::{
        AuditEventKind, AuditEventPayload, AuditGateResult, AuditObservedMarket,
        ExecutionAuditEvent, GateDecisionEvent, MarketMarkEvent, PositionMarkEvent,
        PulseBookLevelAuditRow, PulseBookSnapshotAudit, PulseBookTapeAuditEvent,
        PulseLifecycleAuditEvent, SignalEmittedEvent,
    };
    use polyalpha_core::{
        AsyncClassification, CexBaseQty, EvaluableStatus, Exchange, ExecutionEvent,
        ExecutionResult, Fill, InstrumentKind, MarketConfig, MarketRule, MarketRuleKind,
        MarketTier, OrderLedgerEntry, OrderSide, PolyShares, PolymarketIds, PositionView, Price,
        RecoveryDecisionReason, TokenSide, TradePlan, UsdNotional, VenueQuantity,
        PLANNING_SCHEMA_VERSION,
    };
    use rust_decimal::Decimal;

    #[test]
    fn trade_timeline_list_uses_user_facing_path_labels() {
        let events = sample_trade_timeline_events();

        let trades = collect_closed_trade_items(&events, None);

        assert_eq!(trades.len(), 1);
        assert_eq!(
            trades[0].path,
            "补 CEX 对冲 -> 处理剩余敞口 -> 按实际成交回补对冲"
        );
    }

    #[test]
    fn trade_timeline_detail_marks_mock_timestamp_skew_and_keeps_seq_order() {
        let events = sample_trade_timeline_events();
        let trades = collect_closed_trade_items(&events, None);

        let detail =
            build_trade_timeline_detail(&events, trades.first().expect("sample closed trade"))
                .expect("detail report");

        assert!(detail.skew_detected);
        assert_eq!(detail.rows.first().expect("first row").seq, 10);
        assert_eq!(detail.rows[1].stage, "补 CEX 对冲");
        assert_eq!(detail.rows[1].edge_usd, "-0.0957528456");
        assert_eq!(detail.rows[1].residual_delta, "0.090730227607943");
        assert_eq!(detail.rows.last().expect("last row").stage, "平仓完成");
    }

    #[test]
    fn open_decision_list_collects_open_position_entries() {
        let events = sample_open_decision_events();

        let entries = collect_open_decision_items(&events, None);

        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].symbol,
            "will-ethereum-dip-to-1500-by-december-31-2026"
        );
        assert_eq!(entries[0].token_side, "NO");
        assert_eq!(entries[0].planned_edge_usd, "96.44812734271181406215571349");
        assert_eq!(entries[0].correlation_id, "corr-open-1");
    }

    #[test]
    fn open_decision_detail_reports_rule_pricing_and_sigma_context() {
        let events = sample_open_decision_events();
        let entries = collect_open_decision_items(&events, None);

        let detail = build_open_decision_detail(
            &events,
            &sample_open_decision_markets(),
            entries.first().expect("open decision entry"),
        )
        .expect("open decision detail");

        assert_eq!(detail.market.rule_kind, "低于");
        assert_eq!(detail.market.lower_strike.as_deref(), None);
        assert_eq!(detail.market.upper_strike.as_deref(), Some("1500"));
        assert_eq!(detail.signal.as_ref().expect("signal").token_side, "NO");
        assert_eq!(
            detail.signal.as_ref().expect("signal").candidate_price,
            Some(0.335)
        );
        assert_eq!(
            detail
                .plans
                .first()
                .expect("open plan")
                .instant_open_loss_usd,
            format_decimal(dec("7.9939577799514333655105618889"))
        );
        assert!(detail
            .signal
            .as_ref()
            .expect("signal")
            .raw_sigma_annualized
            .is_some());
        assert_eq!(
            detail
                .signal
                .as_ref()
                .expect("signal")
                .sigma_source
                .as_deref(),
            Some("realized")
        );
        assert!(detail
            .signal
            .as_ref()
            .expect("signal")
            .effective_sigma_annualized
            .is_some());
        assert!(detail
            .latest_market_snapshot
            .as_ref()
            .expect("latest mark")
            .effective_sigma_annualized
            .is_some());
    }

    #[test]
    fn pulse_session_timeline_collects_lifecycle_and_tape_events() {
        let events = vec![
            AuditEvent {
                session_id: "audit-session-1".to_owned(),
                env: "test".to_owned(),
                seq: 1,
                timestamp_ms: 1_000,
                kind: AuditEventKind::PulseLifecycle,
                symbol: None,
                signal_id: None,
                correlation_id: None,
                gate: None,
                result: None,
                reason: None,
                summary: "pulse btc maker_exit_working".to_owned(),
                payload: AuditEventPayload::PulseLifecycle(PulseLifecycleAuditEvent {
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
                }),
            },
            AuditEvent {
                session_id: "audit-session-1".to_owned(),
                env: "test".to_owned(),
                seq: 2,
                timestamp_ms: 1_005,
                kind: AuditEventKind::PulseBookTape,
                symbol: None,
                signal_id: None,
                correlation_id: None,
                gate: None,
                result: None,
                reason: None,
                summary: "pulse tape btc cex_perp".to_owned(),
                payload: AuditEventPayload::PulseBookTape(PulseBookTapeAuditEvent {
                    session_id: "pulse-session-1".to_owned(),
                    asset: "btc".to_owned(),
                    state: "maker_exit_working".to_owned(),
                    symbol: "btc-above-100k".to_owned(),
                    book: PulseBookSnapshotAudit {
                        exchange: "Binance".to_owned(),
                        instrument: "cex_perp".to_owned(),
                        received_at_ms: 1_005,
                        sequence: 42,
                        bids: vec![PulseBookLevelAuditRow {
                            price: "100000".to_owned(),
                            quantity: "1.5".to_owned(),
                        }],
                        asks: vec![PulseBookLevelAuditRow {
                            price: "100010".to_owned(),
                            quantity: "1.2".to_owned(),
                        }],
                    },
                }),
            },
        ];

        let timeline = build_pulse_session_timeline(&events, "pulse-session-1");

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].kind, "Pulse 生命周期");
        assert_eq!(timeline[0].state, "maker_exit_working");
        assert_eq!(timeline[1].kind, "Pulse 盘口 Tape");
        assert_eq!(timeline[1].instrument.as_deref(), Some("cex_perp"));
    }

    #[test]
    fn pulse_raw_opening_kpi_aggregates_effective_open_and_rejections() {
        let events = vec![
            pulse_lifecycle_event(
                1,
                1_000,
                "pulse-session-1",
                "btc",
                "poly_opening",
                "1000",
                "250",
                "62.5",
                "1.2",
                false,
                "partial_fill",
                None,
            ),
            pulse_lifecycle_event(
                2,
                1_100,
                "pulse-session-1",
                "btc",
                "maker_exit_working",
                "1000",
                "250",
                "62.5",
                "1.2",
                true,
                "effective_open",
                None,
            ),
            pulse_lifecycle_event(
                3,
                1_200,
                "pulse-session-2",
                "btc",
                "closed",
                "1200",
                "600",
                "45.225",
                "1.2670888380606710646447687897",
                false,
                "rejected",
                Some("min_open_notional"),
            ),
        ];

        let kpi = collect_pulse_raw_opening_kpi(&events, None);

        assert_eq!(kpi.opening_attempts, 2);
        assert_eq!(kpi.positive_fill_count, 2);
        assert_eq!(kpi.effective_open_count, 1);
        assert_eq!(kpi.still_active_effective_open_count, 1);
        assert_eq!(
            kpi.closed_rejection_reason_counts
                .get("min_open_notional")
                .copied(),
            Some(1)
        );
        assert_eq!(kpi.max_actual_fill_notional_usd.as_deref(), Some("62.5"));
        assert_eq!(
            kpi.max_expected_open_net_pnl_usd.as_deref(),
            Some("1.2670888380606710646447687897")
        );
        assert_eq!(kpi.effective_open_examples.len(), 1);
        assert_eq!(kpi.effective_open_examples[0].session_id, "pulse-session-1");
    }

    #[test]
    fn pulse_raw_opening_kpi_respects_asset_filter() {
        let events = vec![
            pulse_lifecycle_event(
                1,
                1_000,
                "pulse-btc-1",
                "btc",
                "maker_exit_working",
                "1000",
                "250",
                "62.5",
                "1.2",
                true,
                "effective_open",
                None,
            ),
            pulse_lifecycle_event(
                2,
                1_100,
                "pulse-eth-1",
                "eth",
                "maker_exit_working",
                "800",
                "400",
                "80",
                "2.5",
                true,
                "effective_open",
                None,
            ),
        ];

        let kpi = collect_pulse_raw_opening_kpi(&events, Some("eth"));

        assert_eq!(kpi.opening_attempts, 1);
        assert_eq!(kpi.effective_open_count, 1);
        assert_eq!(kpi.effective_open_examples.len(), 1);
        assert_eq!(kpi.effective_open_examples[0].asset, "eth");
        assert_eq!(kpi.effective_open_examples[0].session_id, "pulse-eth-1");
    }

    #[test]
    fn open_rule_kind_label_uses_user_facing_terms() {
        assert_eq!(open_rule_kind_label(MarketRuleKind::Above), "高于");
        assert_eq!(open_rule_kind_label(MarketRuleKind::Below), "低于");
        assert_eq!(open_rule_kind_label(MarketRuleKind::Between), "区间内");
    }

    #[test]
    fn pricing_state_label_reports_warmup_and_realized_in_user_terms() {
        assert_eq!(
            pricing_state_label(Some("default"), 2),
            "参考价预热中（已积累 2 笔，暂用默认波动）"
        );
        assert_eq!(
            pricing_state_label(Some("realized"), 64),
            "已切换实时波动（样本 64 笔）"
        );
    }

    fn sample_trade_timeline_events() -> Vec<AuditEvent> {
        let symbol = "will-solana-dip-to-60-by-december-31-2026";
        let corr = "corr-1";

        vec![
            position_mark_event(10, 2_000, symbol),
            execution_event(
                20,
                5_000,
                symbol,
                corr,
                ExecutionEvent::TradePlanCreated {
                    plan: sample_trade_plan(
                        "plan-root",
                        None,
                        "delta_rebalance",
                        OrderSide::Sell,
                        "1.5",
                        "79.79403800",
                        OrderSide::Sell,
                        "0",
                        "0",
                        "-0.09575284560",
                        0.090730227607943,
                        None,
                    ),
                },
            ),
            execution_event(
                21,
                1_000,
                symbol,
                corr,
                ExecutionEvent::OrderFilled(sample_fill(
                    corr,
                    symbol,
                    Exchange::Binance,
                    InstrumentKind::CexPerp,
                    OrderSide::Sell,
                    "1.5",
                    "79.79403800",
                    "119.691057000",
                    "0.05984552850",
                    1_000,
                )),
            ),
            execution_event(
                22,
                1_000,
                symbol,
                corr,
                ExecutionEvent::ExecutionResultRecorded {
                    result: sample_execution_result(
                        "plan-root",
                        corr,
                        symbol,
                        vec![sample_order_ledger_entry("1.5", "79.79403800", 1_000)],
                        vec![],
                        "-0.09575284560",
                        0.090730227607943,
                        true,
                        1_000,
                    ),
                },
            ),
            execution_event(
                23,
                5_000,
                symbol,
                corr,
                ExecutionEvent::RecoveryPlanCreated {
                    plan: sample_trade_plan(
                        "plan-recovery",
                        Some("plan-root"),
                        "residual_recovery",
                        OrderSide::Buy,
                        "2.6",
                        "79.83596400",
                        OrderSide::Sell,
                        "223.58327857339818524057950471",
                        "0.1805093920708679168049046776",
                        "-7.937542418649986080089979786",
                        0.0,
                        Some(RecoveryDecisionReason::PolyFlattenOnlySafeRoute),
                    ),
                },
            ),
            execution_event(
                24,
                1_100,
                symbol,
                corr,
                ExecutionEvent::OrderFilled(sample_fill(
                    corr,
                    symbol,
                    Exchange::Polymarket,
                    InstrumentKind::PolyNo,
                    OrderSide::Sell,
                    "223.58327857339818524057950471",
                    "0.1805093920708679168049046776",
                    "40.358881692495614976587789306",
                    "0.0807177633849912299531755786",
                    1_100,
                )),
            ),
            execution_event(
                25,
                1_200,
                symbol,
                corr,
                ExecutionEvent::TradePlanCreated {
                    plan: sample_trade_plan(
                        "plan-child",
                        Some("plan-recovery"),
                        "delta_rebalance",
                        OrderSide::Buy,
                        "2.6",
                        "79.83596400",
                        OrderSide::Sell,
                        "223.58327857339818524057950471",
                        "0.1805093920708679168049046776",
                        "-7.958299769289986080089979786",
                        0.0,
                        Some(RecoveryDecisionReason::PolyFlattenOnlySafeRoute),
                    ),
                },
            ),
            execution_event(
                26,
                1_200,
                symbol,
                corr,
                ExecutionEvent::ExecutionResultRecorded {
                    result: sample_execution_result(
                        "plan-child",
                        corr,
                        symbol,
                        vec![sample_order_ledger_entry("2.6", "79.83596400", 1_200)],
                        vec![],
                        "-7.958299769289986080089979786",
                        0.0,
                        false,
                        1_200,
                    ),
                },
            ),
            execution_event(
                27,
                1_200,
                symbol,
                corr,
                ExecutionEvent::TradeClosed {
                    symbol: polyalpha_core::Symbol::new(symbol),
                    correlation_id: corr.to_owned(),
                    realized_pnl: usd("-16.154823353978805884625248443"),
                    timestamp_ms: 1_200,
                },
            ),
        ]
    }

    fn sample_open_decision_events() -> Vec<AuditEvent> {
        let symbol = "will-ethereum-dip-to-1500-by-december-31-2026";
        let corr = "corr-open-1";
        let signal_id = "cand-will-ethereum-dip-to-1500-by-december-31-2026-No-22";

        vec![
            signal_event(
                10,
                1_000,
                symbol,
                signal_id,
                corr,
                sample_open_candidate(symbol, signal_id, corr),
                observed_market(symbol, 0.665, 0.335, 2045.995, EvaluableStatus::Evaluable),
            ),
            gate_event(
                11,
                1_001,
                symbol,
                signal_id,
                corr,
                "ws_freshness",
                AuditGateResult::Passed,
                "ok",
            ),
            gate_event(
                12,
                1_002,
                symbol,
                signal_id,
                corr,
                "price_filter",
                AuditGateResult::Passed,
                "ok",
            ),
            gate_event(
                13,
                1_003,
                symbol,
                signal_id,
                corr,
                "risk_guard",
                AuditGateResult::Passed,
                "ok",
            ),
            market_mark_event(14, 1_004, symbol, 45, 0.335, 2045.995, 0.6313247925172116),
            execution_event(
                15,
                1_005,
                symbol,
                corr,
                ExecutionEvent::TradePlanCreated {
                    plan: sample_open_trade_plan(symbol, corr),
                },
            ),
            execution_event(
                16,
                1_006,
                symbol,
                corr,
                ExecutionEvent::OrderFilled(sample_fill(
                    corr,
                    symbol,
                    Exchange::Polymarket,
                    InstrumentKind::PolyNo,
                    OrderSide::Buy,
                    "469.13423075173636433572673052",
                    "0.3510629823430630054020721700",
                    "164.69566216692326888021857945",
                    "0.3293913243338465377604371589",
                    1_006,
                )),
            ),
            execution_event(
                17,
                1_007,
                symbol,
                corr,
                ExecutionEvent::TradePlanCreated {
                    plan: sample_open_child_rebalance_plan(symbol, corr),
                },
            ),
            execution_event(
                18,
                1_008,
                symbol,
                corr,
                ExecutionEvent::OrderFilled(sample_fill(
                    corr,
                    symbol,
                    Exchange::Binance,
                    InstrumentKind::CexPerp,
                    OrderSide::Sell,
                    "0.09",
                    "2045.580802",
                    "184.10227218",
                    "0.09205113609",
                    1_008,
                )),
            ),
            execution_event(
                19,
                1_009,
                symbol,
                corr,
                ExecutionEvent::ExecutionResultRecorded {
                    result: sample_execution_result(
                        "plan-open-rebalance",
                        corr,
                        symbol,
                        vec![sample_order_ledger_entry("0.09", "2045.580802", 1_008)],
                        vec![],
                        "96.44812734271181406215571349",
                        0.009999999984869123,
                        false,
                        1_009,
                    ),
                },
            ),
        ]
    }

    fn sample_open_decision_markets() -> Vec<MarketConfig> {
        vec![MarketConfig {
            symbol: polyalpha_core::Symbol::new("will-ethereum-dip-to-1500-by-december-31-2026"),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some("Will Ethereum dip to $1,500 by December 31, 2026?".to_owned()),
            market_rule: Some(MarketRule {
                kind: MarketRuleKind::Below,
                lower_strike: None,
                upper_strike: Some(price("1500")),
            }),
            cex_symbol: "ETHUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: None,
            settlement_timestamp: 23_537_320,
            min_tick_size: price("0.001"),
            neg_risk: false,
            cex_price_tick: dec("0.01"),
            cex_qty_step: dec("0.01"),
            cex_contract_multiplier: dec("1"),
        }]
    }

    fn pulse_lifecycle_event(
        seq: u64,
        timestamp_ms: u64,
        pulse_session_id: &str,
        asset: &str,
        state: &str,
        planned_poly_qty: &str,
        actual_poly_filled_qty: &str,
        actual_fill_notional_usd: &str,
        expected_open_net_pnl_usd: &str,
        effective_open: bool,
        opening_outcome: &str,
        opening_rejection_reason: Option<&str>,
    ) -> AuditEvent {
        AuditEvent {
            session_id: "audit-session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::PulseLifecycle,
            symbol: None,
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: None,
            summary: format!("pulse {asset} {state}"),
            payload: AuditEventPayload::PulseLifecycle(PulseLifecycleAuditEvent {
                session_id: pulse_session_id.to_owned(),
                asset: asset.to_owned(),
                state: state.to_owned(),
                entry_price: Some("0.35".to_owned()),
                planned_poly_qty: planned_poly_qty.to_owned(),
                actual_poly_filled_qty: actual_poly_filled_qty.to_owned(),
                actual_poly_fill_ratio: 0.25,
                actual_fill_notional_usd: actual_fill_notional_usd.to_owned(),
                candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
                expected_open_net_pnl_usd: expected_open_net_pnl_usd.to_owned(),
                timeout_loss_estimate_usd: Some("21.68".to_owned()),
                required_hit_rate: Some(0.768),
                pulse_score_bps: Some(182.5),
                target_exit_price: Some("0.38".to_owned()),
                timeout_exit_price: Some("0.31".to_owned()),
                entry_executable_notional_usd: Some("250".to_owned()),
                reversion_pocket_ticks: Some(4.0),
                reversion_pocket_notional_usd: Some("28.57".to_owned()),
                vacuum_ratio: Some("1".to_owned()),
                effective_open,
                opening_outcome: opening_outcome.to_owned(),
                opening_rejection_reason: opening_rejection_reason.map(str::to_owned),
                opening_allocated_hedge_qty: "0.10".to_owned(),
                session_target_delta_exposure: "0.12".to_owned(),
                session_allocated_hedge_qty: "0.10".to_owned(),
                account_net_target_delta_before_order: "0.22".to_owned(),
                account_net_target_delta_after_order: "0.12".to_owned(),
                delta_bump_used: "10".to_owned(),
                anchor_latency_delta_ms: Some(25),
                distance_to_mid_bps: Some(5.0),
                relative_order_age_ms: Some(800),
                poly_yes_book: None,
                poly_no_book: None,
                cex_book: None,
            }),
        }
    }

    fn position_mark_event(seq: u64, timestamp_ms: u64, symbol: &str) -> AuditEvent {
        AuditEvent {
            session_id: "session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::PositionMark,
            symbol: Some(symbol.to_owned()),
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: None,
            summary: "持仓快照".to_owned(),
            payload: AuditEventPayload::PositionMark(PositionMarkEvent {
                tick_index: 241,
                position: PositionView {
                    market: symbol.to_owned(),
                    poly_side: "LONG NO".to_owned(),
                    poly_entry_price: 0.2500062419614994,
                    poly_current_price: 0.215,
                    poly_quantity: 223.5832785733982,
                    poly_pnl_usd: -7.826810348285702,
                    poly_spread_pct: 0.0,
                    poly_slippage_bps: 50.0,
                    cex_exchange: "Binance".to_owned(),
                    cex_side: "SHORT".to_owned(),
                    cex_entry_price: 79.73405,
                    cex_current_price: 79.775,
                    cex_quantity: 1.1,
                    cex_pnl_usd: -0.045045000000010306,
                    cex_spread_pct: 0.0,
                    cex_slippage_bps: 2.0,
                    basis_entry_bps: -7611,
                    basis_current_bps: -7611,
                    delta: 0.005101821441557464,
                    hedge_ratio: 1.0,
                    total_pnl_usd: -7.871855348285712,
                    realized_pnl_so_far_usd: -1.25,
                    close_cost_preview_usd: Some(2.5),
                    exit_now_net_pnl_usd: Some(-11.621855348285712),
                    holding_secs: 241,
                },
            }),
        }
    }

    fn execution_event(
        seq: u64,
        timestamp_ms: u64,
        symbol: &str,
        correlation_id: &str,
        event: ExecutionEvent,
    ) -> AuditEvent {
        AuditEvent {
            session_id: "session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::Execution,
            symbol: Some(symbol.to_owned()),
            signal_id: Some(format!("signal-{correlation_id}")),
            correlation_id: Some(correlation_id.to_owned()),
            gate: None,
            result: None,
            reason: None,
            summary: "执行事件".to_owned(),
            payload: AuditEventPayload::Execution(ExecutionAuditEvent { event }),
        }
    }

    fn signal_event(
        seq: u64,
        timestamp_ms: u64,
        symbol: &str,
        signal_id: &str,
        correlation_id: &str,
        candidate: polyalpha_core::OpenCandidate,
        observed: AuditObservedMarket,
    ) -> AuditEvent {
        AuditEvent {
            session_id: "session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::SignalEmitted,
            symbol: Some(symbol.to_owned()),
            signal_id: Some(signal_id.to_owned()),
            correlation_id: Some(correlation_id.to_owned()),
            gate: None,
            result: None,
            reason: None,
            summary: "开仓机会".to_owned(),
            payload: AuditEventPayload::SignalEmitted(SignalEmittedEvent {
                candidate,
                observed: Some(observed),
            }),
        }
    }

    fn gate_event(
        seq: u64,
        timestamp_ms: u64,
        symbol: &str,
        signal_id: &str,
        correlation_id: &str,
        gate: &str,
        result: AuditGateResult,
        reason: &str,
    ) -> AuditEvent {
        let candidate = sample_open_candidate(symbol, signal_id, correlation_id);
        AuditEvent {
            session_id: "session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::GateDecision,
            symbol: Some(symbol.to_owned()),
            signal_id: Some(signal_id.to_owned()),
            correlation_id: Some(correlation_id.to_owned()),
            gate: Some(gate.to_owned()),
            result: Some(result.as_str().to_owned()),
            reason: Some(reason.to_owned()),
            summary: format!("{gate} {reason}"),
            payload: AuditEventPayload::GateDecision(GateDecisionEvent {
                gate: gate.to_owned(),
                result,
                reason: reason.to_owned(),
                candidate,
                observed: Some(observed_market(
                    symbol,
                    0.665,
                    0.335,
                    2045.995,
                    EvaluableStatus::Evaluable,
                )),
                planning_diagnostics: None,
            }),
        }
    }

    fn market_mark_event(
        seq: u64,
        timestamp_ms: u64,
        symbol: &str,
        tick_index: u64,
        poly_price: f64,
        cex_price: f64,
        fair_value: f64,
    ) -> AuditEvent {
        AuditEvent {
            session_id: "session-1".to_owned(),
            env: "test".to_owned(),
            seq,
            timestamp_ms,
            kind: AuditEventKind::MarketMark,
            symbol: Some(symbol.to_owned()),
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: None,
            summary: "市场快照".to_owned(),
            payload: AuditEventPayload::MarketMark(MarketMarkEvent {
                tick_index,
                market: polyalpha_core::MarketView {
                    symbol: symbol.to_owned(),
                    z_score: Some(-12.0),
                    basis_pct: Some(-29.63247925172116),
                    poly_price: Some(poly_price),
                    poly_yes_price: Some(1.0 - poly_price),
                    poly_no_price: Some(poly_price),
                    cex_price: Some(cex_price),
                    fair_value: Some(fair_value),
                    has_position: true,
                    minutes_to_expiry: Some(392_622.2166666667),
                    basis_history_len: 600,
                    raw_sigma: Some(0.00016),
                    effective_sigma: Some(0.00016),
                    sigma_source: Some("realized".to_owned()),
                    returns_window_len: 64,
                    active_token_side: Some("NO".to_owned()),
                    poly_updated_at_ms: Some(995),
                    cex_updated_at_ms: Some(999),
                    poly_quote_age_ms: Some(20),
                    cex_quote_age_ms: Some(5),
                    cross_leg_skew_ms: Some(15),
                    evaluable_status: EvaluableStatus::Evaluable,
                    async_classification: AsyncClassification::PolyLagAcceptable,
                    market_tier: MarketTier::Tradeable,
                    retention_reason: Default::default(),
                    last_focus_at_ms: None,
                    last_tradeable_at_ms: None,
                },
            }),
        }
    }

    fn sample_open_candidate(
        symbol: &str,
        signal_id: &str,
        correlation_id: &str,
    ) -> polyalpha_core::OpenCandidate {
        polyalpha_core::OpenCandidate {
            schema_version: PLANNING_SCHEMA_VERSION,
            candidate_id: signal_id.to_owned(),
            correlation_id: correlation_id.to_owned(),
            symbol: polyalpha_core::Symbol::new(symbol),
            token_side: TokenSide::No,
            direction: "long".to_owned(),
            fair_value: 0.9692637928375303,
            raw_mispricing: -0.6342637928375303,
            delta_estimate: 0.00021315860883702742,
            risk_budget_usd: 200.0,
            strength: polyalpha_core::SignalStrength::Strong,
            z_score: Some(-78.25764036163268),
            raw_sigma: Some(0.00016),
            effective_sigma: Some(0.00016),
            sigma_source: Some("realized".to_owned()),
            returns_window_len: 64,
            timestamp_ms: 1_000,
        }
    }

    fn observed_market(
        symbol: &str,
        poly_yes_mid: f64,
        poly_no_mid: f64,
        cex_mid: f64,
        evaluable_status: EvaluableStatus,
    ) -> AuditObservedMarket {
        AuditObservedMarket {
            symbol: symbol.to_owned(),
            poly_yes_mid: Some(poly_yes_mid),
            poly_no_mid: Some(poly_no_mid),
            cex_mid: Some(cex_mid),
            poly_updated_at_ms: Some(900),
            cex_updated_at_ms: Some(950),
            poly_quote_age_ms: Some(91),
            cex_quote_age_ms: Some(8),
            cross_leg_skew_ms: Some(83),
            market_phase: Some("Trading".to_owned()),
            connections: HashMap::new(),
            transport_idle_ms_by_connection: HashMap::new(),
            evaluable_status,
            async_classification: AsyncClassification::PolyLagAcceptable,
        }
    }

    fn sample_open_trade_plan(symbol: &str, correlation_id: &str) -> TradePlan {
        TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: "plan-open-root".to_owned(),
            parent_plan_id: None,
            supersedes_plan_id: None,
            idempotency_key: "idem-open-root".to_owned(),
            correlation_id: correlation_id.to_owned(),
            symbol: polyalpha_core::Symbol::new(symbol),
            intent_type: "open_position".to_owned(),
            priority: "open_position".to_owned(),
            recovery_decision_reason: None,
            created_at_ms: 1_005,
            poly_exchange_timestamp_ms: 990,
            poly_received_at_ms: 995,
            poly_sequence: 4,
            cex_exchange_timestamp_ms: 998,
            cex_received_at_ms: 999,
            cex_sequence: 42,
            plan_hash: "hash-open-root".to_owned(),
            poly_side: OrderSide::Buy,
            poly_token_side: TokenSide::No,
            poly_sizing_mode: "buy_budget_cap".to_owned(),
            poly_requested_shares: shares("484.39900637330377803129308365"),
            poly_planned_shares: shares("469.13423075173636433572673052"),
            poly_max_cost_usd: usd("164.69566216692328453063964844"),
            poly_max_avg_price: price("0.3510629823430630054020721700"),
            poly_max_shares: shares("484.39900637330377803129308365"),
            poly_min_avg_price: price("0"),
            poly_min_proceeds_usd: usd("0"),
            poly_book_avg_price: price("0.3493164003413562242806688259"),
            poly_executable_price: price("0.3510629823430630054020721700"),
            poly_friction_cost_usd: usd("7.53569486509158682775012473"),
            poly_fee_usd: usd("0.3293913243338465377604371589"),
            cex_side: OrderSide::Sell,
            cex_planned_qty: cex_qty("0.09"),
            cex_book_avg_price: price("2045.99"),
            cex_executable_price: price("2045.580802"),
            cex_friction_cost_usd: usd("0.036820454436"),
            cex_fee_usd: usd("0.09205113609"),
            raw_edge_usd: usd("104.46049534988124742766627538"),
            planned_edge_usd: usd("96.44812734271181406215571349"),
            expected_funding_cost_usd: usd("0.018410227218"),
            residual_risk_penalty_usd: usd("0"),
            post_rounding_residual_delta: 0.009999999984869123,
            shock_loss_up_1pct: usd("0.2045580798904856730884743155"),
            shock_loss_down_1pct: usd("0.2045580798904856730884743155"),
            shock_loss_up_2pct: usd("0.409116159780971346176948631"),
            shock_loss_down_2pct: usd("0.409116159780971346176948631"),
            plan_ttl_ms: 1_000,
            max_poly_sequence_drift: 2,
            max_cex_sequence_drift: 2,
            max_poly_price_move: price("0.02"),
            max_cex_price_move: price("50"),
            min_planned_edge_usd: usd("0.01"),
            max_residual_delta: 0.05,
            max_shock_loss_usd: usd("200"),
            max_plan_vs_fill_deviation_usd: usd("5"),
        }
    }

    fn sample_open_child_rebalance_plan(symbol: &str, correlation_id: &str) -> TradePlan {
        let mut plan = sample_open_trade_plan(symbol, correlation_id);
        plan.plan_id = "plan-open-rebalance".to_owned();
        plan.parent_plan_id = Some("plan-open-root".to_owned());
        plan.intent_type = "delta_rebalance".to_owned();
        plan.priority = "delta_rebalance".to_owned();
        plan.idempotency_key = "idem-open-rebalance".to_owned();
        plan.plan_hash = "hash-open-rebalance".to_owned();
        plan
    }

    fn sample_trade_plan(
        plan_id: &str,
        parent_plan_id: Option<&str>,
        intent_type: &str,
        cex_side: OrderSide,
        cex_qty_text: &str,
        cex_price: &str,
        poly_side: OrderSide,
        poly_shares: &str,
        poly_price: &str,
        planned_edge: &str,
        residual_delta: f64,
        recovery_reason: Option<RecoveryDecisionReason>,
    ) -> TradePlan {
        TradePlan {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: plan_id.to_owned(),
            parent_plan_id: parent_plan_id.map(str::to_owned),
            supersedes_plan_id: None,
            idempotency_key: format!("idem-{plan_id}"),
            correlation_id: "corr-1".to_owned(),
            symbol: polyalpha_core::Symbol::new("will-solana-dip-to-60-by-december-31-2026"),
            intent_type: intent_type.to_owned(),
            priority: intent_type.to_owned(),
            recovery_decision_reason: recovery_reason,
            created_at_ms: 5_000,
            poly_exchange_timestamp_ms: 5_010,
            poly_received_at_ms: 5_005,
            poly_sequence: 21,
            cex_exchange_timestamp_ms: 5_020,
            cex_received_at_ms: 5_015,
            cex_sequence: 42,
            plan_hash: format!("hash-{plan_id}"),
            poly_side,
            poly_token_side: TokenSide::No,
            poly_sizing_mode: "sell_exact_shares".to_owned(),
            poly_requested_shares: shares(poly_shares),
            poly_planned_shares: shares(poly_shares),
            poly_max_cost_usd: usd("0"),
            poly_max_avg_price: price("0"),
            poly_max_shares: shares(poly_shares),
            poly_min_avg_price: price(poly_price),
            poly_min_proceeds_usd: usd("0"),
            poly_book_avg_price: price(poly_price),
            poly_executable_price: price(poly_price),
            poly_friction_cost_usd: usd("0"),
            poly_fee_usd: usd("0"),
            cex_side,
            cex_planned_qty: cex_qty(cex_qty_text),
            cex_book_avg_price: price(cex_price),
            cex_executable_price: price(cex_price),
            cex_friction_cost_usd: usd("0"),
            cex_fee_usd: usd("0"),
            raw_edge_usd: usd("0"),
            planned_edge_usd: usd(planned_edge),
            expected_funding_cost_usd: usd("0"),
            residual_risk_penalty_usd: usd("0"),
            post_rounding_residual_delta: residual_delta,
            shock_loss_up_1pct: usd("0"),
            shock_loss_down_1pct: usd("0"),
            shock_loss_up_2pct: usd("0"),
            shock_loss_down_2pct: usd("0"),
            plan_ttl_ms: 1_000,
            max_poly_sequence_drift: 2,
            max_cex_sequence_drift: 2,
            max_poly_price_move: price("0.02"),
            max_cex_price_move: price("50"),
            min_planned_edge_usd: usd("0"),
            max_residual_delta: 0.0,
            max_shock_loss_usd: usd("0"),
            max_plan_vs_fill_deviation_usd: usd("0"),
        }
    }

    fn sample_fill(
        correlation_id: &str,
        symbol: &str,
        exchange: Exchange,
        instrument: InstrumentKind,
        side: OrderSide,
        quantity: &str,
        price_value: &str,
        notional: &str,
        fee: &str,
        timestamp_ms: u64,
    ) -> Fill {
        Fill {
            fill_id: format!("fill-{timestamp_ms}"),
            correlation_id: correlation_id.to_owned(),
            exchange,
            symbol: polyalpha_core::Symbol::new(symbol),
            instrument,
            order_id: polyalpha_core::OrderId(format!("order-{timestamp_ms}")),
            side,
            price: price(price_value),
            quantity: match instrument {
                InstrumentKind::CexPerp => VenueQuantity::CexBaseQty(cex_qty(quantity)),
                InstrumentKind::PolyYes | InstrumentKind::PolyNo => {
                    VenueQuantity::PolyShares(shares(quantity))
                }
            },
            notional_usd: usd(notional),
            fee: usd(fee),
            is_maker: false,
            timestamp_ms,
        }
    }

    fn sample_execution_result(
        plan_id: &str,
        correlation_id: &str,
        symbol: &str,
        cex_order_ledger: Vec<OrderLedgerEntry>,
        poly_order_ledger: Vec<OrderLedgerEntry>,
        realized_edge: &str,
        residual_delta: f64,
        recovery_required: bool,
        timestamp_ms: u64,
    ) -> ExecutionResult {
        ExecutionResult {
            schema_version: PLANNING_SCHEMA_VERSION,
            plan_id: plan_id.to_owned(),
            correlation_id: correlation_id.to_owned(),
            symbol: polyalpha_core::Symbol::new(symbol),
            status: "completed".to_owned(),
            poly_order_ledger,
            cex_order_ledger,
            actual_poly_cost_usd: usd("0"),
            actual_cex_cost_usd: usd("0"),
            actual_poly_fee_usd: usd("0"),
            actual_cex_fee_usd: usd("0"),
            actual_funding_cost_usd: usd("0"),
            realized_edge_usd: usd(realized_edge),
            plan_vs_fill_deviation_usd: usd("0"),
            actual_residual_delta: residual_delta,
            actual_shock_loss_up_1pct: usd("0"),
            actual_shock_loss_down_1pct: usd("0"),
            actual_shock_loss_up_2pct: usd("0"),
            actual_shock_loss_down_2pct: usd("0"),
            recovery_required,
            timestamp_ms,
        }
    }

    fn sample_order_ledger_entry(
        filled_qty: &str,
        avg_price: &str,
        timestamp_ms: u64,
    ) -> OrderLedgerEntry {
        OrderLedgerEntry {
            order_id: format!("order-{timestamp_ms}"),
            client_order_id: format!("client-{timestamp_ms}"),
            requested_qty: filled_qty.to_owned(),
            filled_qty: filled_qty.to_owned(),
            cancelled_qty: "0".to_owned(),
            avg_price: avg_price.to_owned(),
            fee_usd: 0.0,
            exchange_timestamp_ms: timestamp_ms,
            received_at_ms: timestamp_ms,
            status: "filled".to_owned(),
        }
    }

    fn dec(value: &str) -> Decimal {
        value.parse().expect("decimal")
    }

    fn price(value: &str) -> Price {
        Price(dec(value))
    }

    fn usd(value: &str) -> UsdNotional {
        UsdNotional(dec(value))
    }

    fn shares(value: &str) -> PolyShares {
        PolyShares(dec(value))
    }

    fn cex_qty(value: &str) -> CexBaseQty {
        CexBaseQty(dec(value))
    }
}
