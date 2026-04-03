use anyhow::{anyhow, Context, Result};
use chrono::{Local, TimeZone};
use polyalpha_audit::{
    AuditEvent, AuditEventPayload, AuditReader, AuditSessionSummary, AuditWarehouse,
    MarketMarkEvent, PositionMarkEvent, SignalEmittedEvent,
};
use polyalpha_core::{
    ExecutionEvent, Exchange, OrderSide, RecoveryDecisionReason, Settings, TokenSide, TradePlan,
    TradeView, VenueQuantity,
};
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
                    serde_json::to_string_pretty(&TradeTimelineDetailReport {
                        summary,
                        ..report
                    })?
                ),
                AuditOutputFormat::Table => print_trade_timeline_detail(
                    &TradeTimelineDetailReport {
                        summary,
                        ..report
                    },
                ),
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

    let first_seq = related_events.first().map(|event| event.seq).unwrap_or_default();
    let pre_close_event = events
        .iter()
        .rev()
        .find(|event| {
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
        },
        trade: selected_trade.clone(),
        skew_detected,
        pre_close,
        rows,
        attribution,
    })
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
            if row.note.is_empty() { "-" } else { row.note.as_str() }
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
        "poly_quote_stale" => "Polymarket 报价已超时".to_owned(),
        "cex_quote_stale" => "交易所报价已超时".to_owned(),
        "misaligned" => "双腿时间未对齐".to_owned(),
        "connection_impaired" => "行情连接异常".to_owned(),
        _ => classification.to_owned(),
    }
}

fn candidate_direction_text(direction: &str) -> &str {
    match direction {
        "long" => "开仓候选",
        "short" => "反向候选",
        _ => direction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_audit::{
        AuditEventKind, AuditEventPayload, ExecutionAuditEvent, PositionMarkEvent,
    };
    use polyalpha_core::{
        CexBaseQty, Exchange, ExecutionEvent, ExecutionResult, Fill, InstrumentKind,
        OrderLedgerEntry, OrderSide, PolyShares, PositionView, Price, RecoveryDecisionReason,
        TokenSide, TradePlan, UsdNotional, VenueQuantity, PLANNING_SCHEMA_VERSION,
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
