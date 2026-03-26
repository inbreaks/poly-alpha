use anyhow::{anyhow, Context, Result};
use chrono::{Local, TimeZone};
use polyalpha_audit::{
    AuditEvent, AuditEventPayload, AuditReader, AuditSessionSummary, AuditWarehouse,
    MarketMarkEvent, PositionMarkEvent, SignalEmittedEvent,
};
use polyalpha_core::{ArbSignalAction, Settings, TradeView};
use serde::Serialize;
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
        .map(|signal| signal.signal.signal_id.as_str());
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
                    "最近信号: {} {} @ {}",
                    signal.signal.signal_id,
                    signal_action_text(&signal.signal.action),
                    format_timestamp_ms(signal.signal.timestamp_ms)
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
                println!("最近信号: 无");
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

fn signal_action_text(action: &ArbSignalAction) -> &'static str {
    match action {
        ArbSignalAction::BasisLong { .. } => "基差做多",
        ArbSignalAction::BasisShort { .. } => "基差做空",
        ArbSignalAction::DeltaRebalance { .. } => "Delta 再平衡",
        ArbSignalAction::NegRiskArb { .. } => "负风险套利",
        ArbSignalAction::ClosePosition { .. } => "平仓",
    }
}
