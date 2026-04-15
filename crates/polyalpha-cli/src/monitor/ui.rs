//! UI 渲染组件

use chrono::{Local, TimeZone};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};
use std::collections::HashMap;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use polyalpha_core::{
    AsyncClassification, CommandKind, CommandStatus, ConnectionStatus, EvaluableStatus, EventKind,
    MarketRetentionReason, MarketTier, MarketView, PulseMarketMonitorRow, TradeView,
};

use super::state::{BannerLevel, Dialog, LogPane, Page, TuiState};

pub fn render_header(state: &TuiState) -> Paragraph<'static> {
    let mode = match state.monitor.mode {
        polyalpha_core::TradingMode::Paper => "模拟盘",
        polyalpha_core::TradingMode::Live => "实盘",
        polyalpha_core::TradingMode::Backtest => "回测",
    };
    let (market_data_mode, market_data_color) =
        market_data_mode_badge(&state.monitor.config.market_data_mode);
    let (status_text, status_color) = if state.monitor.emergency {
        ("紧急停止", Color::Red)
    } else if state.monitor.paused {
        ("已暂停", Color::Yellow)
    } else {
        ("运行中", Color::Green)
    };

    let mut line = vec![
        Span::styled("POLYALPHA", Style::default().fg(Color::Cyan).bold()),
        Span::raw("  "),
        Span::styled(mode, Style::default().fg(Color::White)),
        Span::raw("  "),
        Span::styled(
            format!("行情 {market_data_mode}"),
            Style::default().fg(market_data_color),
        ),
        Span::raw("  "),
        Span::styled(
            format!("运行 {}", format_duration(state.uptime_secs())),
            Style::default().fg(Color::Gray),
        ),
        Span::raw("  "),
        Span::styled(status_text, Style::default().fg(status_color).bold()),
    ];

    if let Some(state_age_ms) = state.state_age_ms() {
        line.push(Span::raw("  "));
        line.push(Span::styled(
            format!("状态 {}", format_age(state_age_ms)),
            Style::default()
                .fg(age_color(state_age_ms))
                .add_modifier(if state.state_is_stale() {
                    ratatui::style::Modifier::BOLD
                } else {
                    ratatui::style::Modifier::empty()
                }),
        ));
    } else {
        line.push(Span::raw("  "));
        line.push(Span::styled(
            "等待首帧",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if state.pending_command_count() > 0 {
        line.push(Span::raw("  "));
        line.push(Span::styled(
            format!("待处理 {}", state.pending_command_count()),
            Style::default().fg(Color::Yellow),
        ));
    }

    if let Some(banner) = &state.banner {
        line.push(Span::raw("  "));
        line.push(Span::styled(
            truncate_display(&banner.text, 72),
            Style::default().fg(color_for_banner(banner.level)),
        ));
    }

    Paragraph::new(Line::from(line)).block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray)),
    )
}

pub fn render_performance(state: &TuiState) -> Paragraph<'static> {
    let perf = &state.monitor.performance;
    let total_color = pnl_color(perf.total_pnl_usd);
    let today_color = pnl_color(perf.today_pnl_usd);
    let equity_delta = perf.equity - perf.initial_capital;
    let (market_data_mode, market_data_color) =
        market_data_mode_badge(&state.monitor.config.market_data_mode);
    let per_asset_summary = state
        .monitor
        .config
        .per_asset
        .iter()
        .map(|item| {
            format!(
                "{} {:.2}-{:.2} Z{:.1}/{:.1} 窗口{}s{}{}",
                item.asset,
                item.min_poly_price,
                item.max_poly_price,
                item.entry_z,
                item.exit_z,
                item.rolling_window,
                if item.enable_freshness_check {
                    " 新鲜度开"
                } else {
                    " 新鲜度关"
                },
                if item.reject_on_disconnect {
                    " 断连拒单"
                } else {
                    ""
                }
            )
        })
        .collect::<Vec<_>>()
        .join("  |  ");

    let mut lines = vec![
        Line::from(vec![
            Span::styled("总收益 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(perf.total_pnl_usd),
                Style::default().fg(total_color).bold(),
            ),
            Span::raw("  "),
            Span::styled("今日 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(perf.today_pnl_usd),
                Style::default().fg(today_color),
            ),
            Span::raw("  "),
            Span::styled("净值 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("${:.2}", perf.equity),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::styled("胜率 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.1}%", perf.win_rate_pct)),
            Span::raw("  "),
            Span::styled("回撤 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.1}%", perf.max_drawdown_pct)),
            Span::raw("  "),
            Span::styled("盈亏比 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if perf.profit_factor.is_finite() {
                format!("{:.2}", perf.profit_factor)
            } else {
                "∞".to_owned()
            }),
            Span::raw("  "),
            Span::styled("会话收益 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(equity_delta),
                Style::default().fg(pnl_color(equity_delta)),
            ),
        ]),
    ];

    lines.extend(strategy_health_summary_lines(state));
    lines.extend(pulse_summary_lines(state));
    lines.extend([
        Line::from(vec![
            Span::styled("参数 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "入场Z {:.2}  出场Z {:.2}  仓位 ${:.0}  窗口 {}s",
                state.monitor.config.entry_z,
                state.monitor.config.exit_z,
                state.monitor.config.position_notional_usd,
                state.monitor.config.rolling_window,
            )),
            Span::raw("  "),
            Span::styled("价格过滤 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} {:.2}-{:.2}",
                state.monitor.config.band_policy,
                state.monitor.config.min_poly_price,
                state.monitor.config.max_poly_price,
            )),
        ]),
        Line::from(vec![
            Span::styled("币种参数 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if per_asset_summary.is_empty() {
                "跟随全局默认".to_owned()
            } else {
                per_asset_summary
            }),
        ]),
        Line::from(vec![
            Span::styled("行情 ", Style::default().fg(Color::DarkGray)),
            Span::styled(market_data_mode, Style::default().fg(market_data_color)),
            Span::raw("  "),
            Span::styled("开仓阈值 ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                if state
                    .monitor
                    .config
                    .market_data_mode
                    .eq_ignore_ascii_case("ws")
                    && state.monitor.config.poly_open_max_quote_age_ms > 0
                {
                    format!(
                        "Poly<= {}ms  CEX<= {}ms  时差<= {}ms",
                        state.monitor.config.poly_open_max_quote_age_ms,
                        state.monitor.config.cex_open_max_quote_age_ms,
                        state.monitor.config.max_cross_leg_skew_ms,
                    )
                } else {
                    "按轮询节奏".to_owned()
                },
            ),
            Span::raw("  "),
            Span::styled("滑点 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "Polymarket {}bps  交易所 {}bps",
                state.monitor.config.poly_slippage_bps, state.monitor.config.cex_slippage_bps,
            )),
        ]),
        Line::from(vec![
            Span::styled("风控 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "总敞口 ${:.0}  单市场 ${:.0}  日亏损 ${:.0}  开单 {}  速率 {}/s",
                state.monitor.config.max_total_exposure_usd,
                state.monitor.config.max_single_position_usd,
                state.monitor.config.max_daily_loss_usd,
                state.monitor.config.max_open_orders,
                state.monitor.config.rate_limit_orders_per_sec,
            )),
            Span::raw("  "),
            Span::styled("连断即拒 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if state.monitor.config.reject_on_disconnect {
                "是".to_owned()
            } else {
                "否".to_owned()
            }),
            Span::raw("  "),
            Span::styled("部分成交 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if state.monitor.config.allow_partial_fill {
                "允许".to_owned()
            } else {
                "禁用".to_owned()
            }),
        ]),
    ]);
    lines.push(recent_trade_summary_line(state));

    Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 业绩 ")
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true })
}

fn recent_trade_summary_line(state: &TuiState) -> Line<'static> {
    let Some(trade) = state.monitor.recent_trades.first() else {
        return Line::from(vec![
            Span::styled("最近平仓 ", Style::default().fg(Color::DarkGray)),
            Span::styled("暂无", Style::default().fg(Color::DarkGray)),
        ]);
    };

    Line::from(vec![
        Span::styled("最近平仓 ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_time(trade.closed_at_ms),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw("  "),
        Span::styled(
            truncate_display(&trade.market, 32),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("  "),
        Span::styled(
            format_signed_usd(trade.realized_pnl_usd),
            Style::default()
                .fg(pnl_color(trade.realized_pnl_usd))
                .bold(),
        ),
        Span::raw("  "),
        Span::styled("入 ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_option_price(trade.entry_price)),
        Span::raw("  "),
        Span::styled("出 ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_option_price(trade.exit_price)),
    ])
}

fn strategy_health_summary_lines(state: &TuiState) -> Vec<Line<'static>> {
    let health = &state.monitor.runtime.strategy_health;
    if health.total_markets == 0 {
        return Vec::new();
    }

    vec![
        Line::from(vec![
            Span::styled("健康 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "Z值就绪 {}/{}",
                    health.z_score_ready_markets, health.total_markets
                ),
                Style::default().fg(if health.z_score_ready_markets == 0 {
                    Color::Red
                } else {
                    Color::Green
                }),
            ),
            Span::raw("  "),
            Span::styled("公允值就绪 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{}/{}",
                health.fair_value_ready_markets, health.total_markets
            )),
            Span::raw("  "),
            Span::styled("预热完成 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}/{}", health.warmup_ready_markets, health.total_markets),
                Style::default().fg(if health.warmup_ready_markets == 0 {
                    Color::Yellow
                } else {
                    Color::Cyan
                }),
            ),
            Span::raw("  "),
            Span::styled("预热进度 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{}/{}",
                health.warmup_completed_markets, health.warmup_total_markets
            )),
            Span::raw("  "),
            Span::styled("预热不足 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", health.insufficient_warmup_markets)),
        ]),
        Line::from(vec![
            Span::styled("链路 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("Stale {}", health.stale_markets)),
            Span::raw("  "),
            Span::raw(format!("NoPoly {}", health.no_poly_markets)),
            Span::raw("  "),
            Span::raw(format!(
                "历史失败 CEX {}/Poly {}",
                health.cex_history_failed_markets, health.poly_history_failed_markets
            )),
            Span::raw("  "),
            Span::styled("门槛 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", health.min_warmup_samples)),
        ]),
    ]
}

fn pulse_summary_lines(state: &TuiState) -> Vec<Line<'static>> {
    let Some(pulse) = state.monitor.pulse.as_ref() else {
        return Vec::new();
    };

    let mut lines = vec![Line::from(vec![
        Span::styled("Pulse ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("会话 {}", pulse.active_sessions.len())),
        Span::raw("  "),
        Span::styled("活跃 ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("{}", pulse.active_sessions.len())),
        Span::raw("  "),
        Span::raw(
            pulse
                .active_sessions
                .iter()
                .map(|row| {
                    format!(
                        "{} {} {:.1}bps {}s",
                        row.asset, row.state, row.net_edge_bps, row.remaining_secs
                    )
                })
                .collect::<Vec<_>>()
                .join(" | "),
        ),
    ])];

    if !pulse.asset_health.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("Pulse 健康 ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                pulse
                    .asset_health
                    .iter()
                    .map(|row| {
                        format!(
                            "{} anchor{} lag{} poly{} cex{} open{} netΔ{} pos{}{}",
                            row.asset,
                            row.anchor_age_ms
                                .map(|value| format!("{value}ms"))
                                .unwrap_or_else(|| "-".to_owned()),
                            row.anchor_latency_delta_ms
                                .map(|value| format!("{value}ms"))
                                .unwrap_or_else(|| "-".to_owned()),
                            row.poly_quote_age_ms
                                .map(|value| format!("{value}ms"))
                                .unwrap_or_else(|| "-".to_owned()),
                            row.cex_quote_age_ms
                                .map(|value| format!("{value}ms"))
                                .unwrap_or_else(|| "-".to_owned()),
                            row.open_sessions,
                            row.net_target_delta
                                .map(|value| format!("{value:.2}"))
                                .unwrap_or_else(|| "-".to_owned()),
                            row.actual_exchange_position
                                .map(|value| format!("{value:.2}"))
                                .unwrap_or_else(|| "-".to_owned()),
                            pulse_asset_health_label(
                                row.status.as_deref(),
                                row.disable_reason.as_deref(),
                            ),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" | "),
            ),
        ]));
    }

    if let Some(session) = pulse.selected_session.as_ref() {
        lines.push(Line::from(vec![
            Span::styled("Pulse 选中 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} {} edge{:.1}bps EV${} 超亏${} Hit{} 入{} 目{} 超{} pocket{}/$ {} 真空{}",
                session.asset,
                session.state,
                session.net_edge_bps,
                session
                    .candidate_expected_net_pnl_usd
                    .as_deref()
                    .unwrap_or("--"),
                session
                    .timeout_loss_estimate_usd
                    .as_deref()
                    .unwrap_or("--"),
                format_optional_ratio_pct(session.required_hit_rate),
                session.entry_price.as_deref().unwrap_or("--"),
                session.target_exit_price.as_deref().unwrap_or("--"),
                session.timeout_exit_price.as_deref().unwrap_or("--"),
                session
                    .reversion_pocket_ticks
                    .map(|value| format!("{value:.1}"))
                    .unwrap_or_else(|| "--".to_owned()),
                session
                    .reversion_pocket_notional_usd
                    .as_deref()
                    .unwrap_or("--"),
                session.vacuum_ratio.as_deref().unwrap_or("--"),
            )),
        ]));
    }

    lines
}

fn strategy_root_cause_line(state: &TuiState) -> Option<Line<'static>> {
    let health = &state.monitor.runtime.strategy_health;
    if health.total_markets == 0 || state.monitor.signals.seen > 0 {
        return None;
    }

    let root_cause =
        if health.cex_history_failed_markets > 0 || health.poly_history_failed_markets > 0 {
            format!(
                "历史抓取异常：CEX {} 个，Poly {} 个",
                health.cex_history_failed_markets, health.poly_history_failed_markets
            )
        } else if health.z_score_ready_markets == 0 {
            format!(
                "Z值未形成，预热完成 {}/{}，门槛 {}",
                health.warmup_ready_markets, health.total_markets, health.min_warmup_samples
            )
        } else if health.stale_markets > 0 || health.no_poly_markets > 0 {
            format!(
                "Stale 市场 {} 个，NoPoly 市场 {} 个",
                health.stale_markets, health.no_poly_markets
            )
        } else {
            return None;
        };

    Some(Line::from(vec![
        Span::styled("当前无信号主因 ", Style::default().fg(Color::DarkGray)),
        Span::styled(root_cause, Style::default().fg(Color::Yellow)),
    ]))
}

pub fn render_signals(state: &TuiState) -> Paragraph<'static> {
    let mut lines = Vec::new();
    if let Some(line) = strategy_root_cause_line(state) {
        lines.push(line);
    }
    let evaluation = &state.monitor.evaluation;
    if evaluation.attempts > 0 {
        let skip_rate = if evaluation.attempts > 0 {
            evaluation.skipped as f64 * 100.0 / evaluation.attempts as f64
        } else {
            0.0
        };
        let signal_reject_rate = if state.monitor.signals.seen > 0 {
            state.monitor.signals.rejected as f64 * 100.0 / state.monitor.signals.seen as f64
        } else {
            0.0
        };
        lines.push(Line::from(vec![
            Span::styled("评估 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} 次", evaluation.attempts)),
            Span::raw("  "),
            Span::styled("可定价 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{} 次", evaluation.evaluable_passes),
                Style::default().fg(Color::Green),
            ),
            Span::raw("  "),
            Span::styled("跳过 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{} 次 ({skip_rate:.1}%)", evaluation.skipped),
                Style::default().fg(skip_rate_color(skip_rate)),
            ),
            Span::raw("  "),
            Span::styled("信号 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} 次", state.monitor.signals.seen)),
            Span::raw("  "),
            Span::styled("拒绝 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "{} 次 ({signal_reject_rate:.1}%)",
                    state.monitor.signals.rejected
                ),
                Style::default().fg(skip_rate_color(signal_reject_rate)),
            ),
            Span::raw("  "),
            Span::styled("执行拒绝 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} 次", evaluation.execution_rejected)),
        ]));

        if let Some((status_key, count)) = top_count_key(&evaluation.evaluable_status_counts) {
            lines.push(Line::from(vec![
                Span::styled("主要状态 ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    truncate_display(evaluable_status_label(status_key), 28),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw("  "),
                Span::styled("次数 ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{count}")),
            ]));
        }

        if let Some((classification_key, count)) =
            top_count_key(&evaluation.async_classification_counts)
        {
            lines.push(Line::from(vec![
                Span::styled("主要异步 ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    truncate_display(async_classification_label(classification_key), 28),
                    Style::default().fg(Color::Yellow),
                ),
                Span::raw("  "),
                Span::styled("次数 ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{count}")),
            ]));
        }

        if let Some((reason_key, count)) = top_count_key(&evaluation.skip_reasons) {
            lines.push(Line::from(vec![
                Span::styled("主要跳过 ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    truncate_display(skip_reason_label(reason_key), 28),
                    Style::default().fg(Color::LightBlue),
                ),
                Span::raw("  "),
                Span::styled("次数 ", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{count}")),
            ]));
        }
    }

    for event in state.monitor.recent_events.iter().take(2) {
        let (label, color) = event_kind_label(event.kind);
        lines.push(Line::from(vec![
            Span::styled(
                format_time(event.timestamp_ms),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(" "),
            Span::styled(format!("[{label}]"), Style::default().fg(color)),
            Span::raw(" "),
            Span::raw(truncate_display(&event.summary, 96)),
        ]));
    }

    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "等待评估与事件流...",
            Style::default().fg(Color::DarkGray),
        )));
    }

    Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 最近信号 / 事件 ")
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true })
}

pub fn render_positions_summary(state: &TuiState) -> Table<'static> {
    let positions = &state.monitor.positions;
    if positions.is_empty() {
        return empty_table(" 持仓概览 ", "暂无持仓");
    }

    let header = table_header([
        left_cell("市场"),
        centered_cell("方向"),
        right_cell("总盈亏"),
    ]);
    let rows = positions.iter().take(6).map(|position| {
        let direction = short_position_side(&position.poly_side);
        Row::new([
            left_cell(truncate_display(&position.market, 24)),
            centered_styled_cell(
                direction,
                Style::default().fg(position_color(&position.poly_side)),
            ),
            right_styled_cell(
                fit_display_right(&format_signed_usd(position.total_pnl_usd), 10),
                Style::default().fg(pnl_color(position.total_pnl_usd)),
            ),
        ])
    });

    Table::new(
        rows,
        [
            Constraint::Min(12),
            Constraint::Length(4),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(format!(" 持仓概览 ({}) ", positions.len())))
}

pub fn render_markets(state: &TuiState) -> Table<'static> {
    if let Some(pulse_rows) = state
        .monitor
        .pulse
        .as_ref()
        .filter(|pulse| !pulse.markets.is_empty())
        .map(|pulse| &pulse.markets)
    {
        return render_pulse_markets(state, pulse_rows);
    }

    let sorted = sorted_markets(&state.monitor.markets);
    if sorted.is_empty() {
        return empty_table(" 市场 ", "暂无市场数据");
    }

    let header = table_header([
        left_cell(""),
        left_cell("市场"),
        right_cell("Z值"),
        right_cell("基差"),
        right_cell("YES价"),
        right_cell("NO价"),
        right_cell("对冲价"),
        right_cell("公允价"),
        right_cell("报价龄(P/C)"),
        right_cell("时差"),
        right_cell("状态"),
    ]);

    let rows = sorted.iter().take(12).enumerate().map(|(row_idx, market)| {
        let selected = row_idx == state.selected_market;
        let row_style = selected_row_style(selected);
        let z_color = match market.z_score {
            Some(value) if value <= -3.0 => Color::Red,
            Some(value) if value <= -1.0 => Color::Yellow,
            Some(_) => Color::Gray,
            None => Color::DarkGray,
        };
        let poly_age_ms = market
            .poly_quote_age_ms
            .or_else(|| state.data_age_ms(market.poly_updated_at_ms));
        let cex_age_ms = market
            .cex_quote_age_ms
            .or_else(|| state.data_age_ms(market.cex_updated_at_ms));
        let min_warmup_samples = state.monitor.runtime.strategy_health.min_warmup_samples as usize;
        let status = format_market_status_compact(market, min_warmup_samples);
        let status_color = market_status_color(market, min_warmup_samples);

        Row::new([
            left_styled_cell(
                if selected { "›" } else { " " },
                Style::default().fg(Color::Cyan),
            ),
            left_cell(truncate_display(&market.symbol, 18)),
            right_styled_cell(
                fit_display_right(&format_optional_metric(market.z_score), 7),
                Style::default().fg(z_color),
            ),
            right_cell(fit_display_right(&format_optional_pct(market.basis_pct), 8)),
            right_cell(fit_display_right(
                &format_optional_price(market.poly_yes_price),
                6,
            )),
            right_cell(fit_display_right(
                &format_optional_price(market.poly_no_price),
                6,
            )),
            right_cell(fit_display_right(
                &format_optional_price(market.cex_price),
                7,
            )),
            right_cell(fit_display_right(
                &format_optional_price(market.fair_value),
                6,
            )),
            right_styled_cell(
                fit_display_right(&format_market_age(poly_age_ms, cex_age_ms), 11),
                Style::default().fg(combined_age_color(poly_age_ms, cex_age_ms)),
            ),
            right_styled_cell(
                fit_display_right(&format_optional_age(market.cross_leg_skew_ms), 6),
                Style::default().fg(age_color(market.cross_leg_skew_ms.unwrap_or_default())),
            ),
            right_styled_cell(
                fit_display_right(&status, 18),
                Style::default().fg(status_color),
            ),
        ])
        .style(row_style)
    });

    Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(11),
            Constraint::Length(6),
            Constraint::Length(18),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(format!(
        " 市场 ({}) ",
        state.monitor.markets.len()
    )))
}

fn render_pulse_markets(state: &TuiState, markets: &[PulseMarketMonitorRow]) -> Table<'static> {
    let header = table_header([
        left_cell(""),
        left_cell("市场"),
        centered_cell("方向"),
        right_cell("Pulse"),
        right_cell("净边"),
        right_cell("EV$"),
        right_cell("超亏$"),
        right_cell("Hit%"),
        right_cell("入/目"),
        right_cell("Pocket"),
        right_cell("真空"),
        right_cell("龄(P/C/A)"),
        right_cell("状态"),
    ]);

    let rows = markets
        .iter()
        .take(12)
        .enumerate()
        .map(|(row_idx, market)| {
            let selected = row_idx == state.selected_market;
            let status_color = pulse_market_status_color(market);
            let side_color = pulse_claim_side_color(market.claim_side.as_deref());
            let age_summary = format!(
                "{}/{}/{}",
                format_optional_age(market.poly_quote_age_ms),
                format_optional_age(market.cex_quote_age_ms),
                format_optional_age(market.anchor_age_ms),
            );

            Row::new([
                left_styled_cell(
                    if selected { "›" } else { " " },
                    Style::default().fg(Color::Cyan),
                ),
                left_cell(truncate_display(&market.symbol, 18)),
                centered_styled_cell(
                    market.claim_side.clone().unwrap_or_else(|| "--".to_owned()),
                    Style::default().fg(side_color),
                ),
                right_cell(fit_display_right(
                    &format_optional_metric(market.pulse_score_bps),
                    7,
                )),
                right_cell(fit_display_right(
                    &format_optional_metric(market.net_edge_bps),
                    7,
                )),
                right_cell(fit_display_right(
                    &format_optional_metric(market.expected_net_pnl_usd),
                    7,
                )),
                right_cell(fit_display_right(
                    &format_optional_metric(market.timeout_loss_estimate_usd),
                    7,
                )),
                right_cell(fit_display_right(
                    &format_optional_ratio_pct(market.required_hit_rate),
                    6,
                )),
                right_cell(fit_display_right(
                    &format_entry_target_pair(market.entry_price, market.target_exit_price),
                    11,
                )),
                right_cell(fit_display_right(
                    &format_pocket_summary(
                        market.reversion_pocket_ticks,
                        market.reversion_pocket_notional_usd,
                    ),
                    11,
                )),
                right_cell(fit_display_right(
                    &format_optional_metric(market.vacuum_ratio),
                    6,
                )),
                right_styled_cell(
                    fit_display_right(&age_summary, 13),
                    Style::default().fg(combined_age_color(
                        market
                            .poly_quote_age_ms
                            .into_iter()
                            .chain(market.cex_quote_age_ms)
                            .chain(market.anchor_age_ms)
                            .max(),
                        None,
                    )),
                ),
                right_styled_cell(
                    fit_display_right(pulse_market_status_label(market), 14),
                    Style::default().fg(status_color),
                ),
            ])
            .style(selected_row_style(selected))
        });

    Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(4),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(6),
            Constraint::Length(13),
            Constraint::Length(14),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(format!(" 市场 ({}) ", markets.len())))
}

fn pulse_market_status_color(market: &PulseMarketMonitorRow) -> Color {
    match market
        .disable_reason
        .as_deref()
        .unwrap_or(market.status.as_str())
    {
        "hedge_drift" => Color::Yellow,
        "ready" => Color::Green,
        "degraded" => Color::Yellow,
        "expired" => Color::Red,
        "anchor_stale" | "anchor_latency_delta_high" => Color::Yellow,
        "residual_hedge" => Color::Yellow,
        "waiting_anchor" | "waiting_books" => Color::DarkGray,
        _ => Color::Gray,
    }
}

fn pulse_claim_side_color(side: Option<&str>) -> Color {
    match side {
        Some("NO") => Color::Green,
        Some("YES") => Color::Red,
        _ => Color::Gray,
    }
}

fn pulse_market_status_label(market: &PulseMarketMonitorRow) -> &'static str {
    pulse_status_label(
        Some(market.status.as_str()),
        market.disable_reason.as_deref(),
    )
}

fn pulse_asset_health_label(status: Option<&str>, disable_reason: Option<&str>) -> String {
    format!(" {}", pulse_status_label(status, disable_reason))
}

fn pulse_status_label(status: Option<&str>, disable_reason: Option<&str>) -> &'static str {
    match disable_reason.or(status) {
        Some("ready") => "可定价",
        Some("enabled") | Some("healthy") => "正常",
        Some("warning") => "注意",
        Some("waiting_anchor") => "等待期权锚",
        Some("anchor_surface_insufficient") => "期权锚缺面",
        Some("anchor_stale") => "期权锚过期",
        Some("anchor_latency_delta_high") => "期权锚滞后",
        Some("waiting_books") | Some("waiting_poly_quote") | Some("waiting_cex_quote") => {
            "等待盘口"
        }
        Some("poly_quote_stale") => "Poly 过旧",
        Some("cex_quote_stale") => "CEX 过旧",
        Some("residual_hedge") => "残余对冲仓位",
        Some("hedge_drift") => "对冲偏移",
        Some("expired") => "已到期",
        Some("disabled") => "已停用",
        Some("degraded") => "降级",
        Some("no_markets") | Some("no_future_markets") => "无可用市场",
        _ => "--",
    }
}

pub fn render_position_list(state: &TuiState) -> Table<'static> {
    let positions = &state.monitor.positions;
    if positions.is_empty() {
        return empty_table(" 持仓列表 ", "当前没有持仓");
    }

    let (start, end) = visible_window(positions.len(), state.selected_position, 10);
    let header = table_header([
        left_cell(""),
        left_cell("市场"),
        centered_cell("方向"),
        right_cell("持仓盈亏"),
    ]);
    let rows = positions
        .iter()
        .enumerate()
        .skip(start)
        .take(end - start)
        .map(|(idx, position)| {
            let selected = idx == state.selected_position;
            Row::new([
                left_styled_cell(
                    if selected { "›" } else { " " },
                    Style::default().fg(Color::Cyan),
                ),
                left_cell(truncate_display(&position.market, 24)),
                centered_styled_cell(
                    short_position_side(&position.poly_side),
                    Style::default().fg(position_color(&position.poly_side)),
                ),
                right_styled_cell(
                    fit_display_right(&format_signed_usd(position.total_pnl_usd), 10),
                    Style::default().fg(pnl_color(position.total_pnl_usd)),
                ),
            ])
            .style(selected_row_style(selected))
        });

    Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Min(12),
            Constraint::Length(4),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(format!(" 持仓列表 ({}) ", positions.len())))
}

pub fn render_position_detail(state: &TuiState) -> Paragraph<'static> {
    let Some(position) = state.selected_position() else {
        return Paragraph::new("选择一条持仓后，这里会显示两腿明细、基差、Delta 和持仓时长。")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" 持仓详情 ")
                    .title_style(Style::default().fg(Color::Cyan)),
            )
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("市场 ", Style::default().fg(Color::DarkGray)),
            Span::raw(position.market.clone()),
            Span::raw("  "),
            Span::styled("持仓时长 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_duration(position.holding_secs)),
        ]),
        Line::from(vec![
            Span::styled("Polymarket ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                display_poly_side(&position.poly_side),
                Style::default().fg(position_color(&position.poly_side)),
            ),
            Span::raw("  "),
            Span::styled("入场 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_price(position.poly_entry_price)),
            Span::raw("  "),
            Span::styled("当前 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_price(position.poly_current_price)),
            Span::raw("  "),
            Span::styled("数量 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.4}", position.poly_quantity)),
        ]),
        Line::from(vec![
            Span::styled("交易所 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} {}",
                position.cex_exchange,
                display_position_side(&position.cex_side)
            )),
            Span::raw("  "),
            Span::styled("入场 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_price(position.cex_entry_price)),
            Span::raw("  "),
            Span::styled("当前 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_price(position.cex_current_price)),
            Span::raw("  "),
            Span::styled("数量 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.6}", position.cex_quantity)),
        ]),
        Line::from(vec![
            Span::styled("Polymarket盈亏 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(position.poly_pnl_usd),
                Style::default().fg(pnl_color(position.poly_pnl_usd)),
            ),
            Span::raw("  "),
            Span::styled("交易所盈亏 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(position.cex_pnl_usd),
                Style::default().fg(pnl_color(position.cex_pnl_usd)),
            ),
            Span::raw("  "),
            Span::styled("盯市总盈亏 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(position.total_pnl_usd),
                Style::default()
                    .fg(pnl_color(position.total_pnl_usd))
                    .bold(),
            ),
        ]),
        Line::from(vec![
            Span::styled("已实现盈亏 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(position.realized_pnl_so_far_usd),
                Style::default().fg(pnl_color(position.realized_pnl_so_far_usd)),
            ),
            Span::raw("  "),
            Span::styled("立即平仓毛值 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_signed_usd(position.realized_pnl_so_far_usd + position.total_pnl_usd),
                Style::default().fg(pnl_color(
                    position.realized_pnl_so_far_usd + position.total_pnl_usd,
                )),
            ),
            Span::raw("  "),
            Span::styled("平仓预估成本 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_optional_cost_usd(position.close_cost_preview_usd),
                Style::default().fg(if position.close_cost_preview_usd.is_some() {
                    Color::Yellow
                } else {
                    Color::DarkGray
                }),
            ),
        ]),
        Line::from(vec![
            Span::styled("立即平仓净值 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_optional_signed_usd(position.exit_now_net_pnl_usd),
                Style::default().fg(position
                    .exit_now_net_pnl_usd
                    .map(pnl_color)
                    .unwrap_or(Color::DarkGray)),
            ),
            Span::raw("  "),
            Span::styled("说明 ", Style::default().fg(Color::DarkGray)),
            Span::styled("毛值=已实现+盯市", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled("入场基差 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} bps", position.basis_entry_bps)),
            Span::raw("  "),
            Span::styled("当前基差 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} bps", position.basis_current_bps)),
            Span::raw("  "),
            Span::styled("Delta敞口 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.4}", position.delta)),
            Span::raw("  "),
            Span::styled("对冲比 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.3}", position.hedge_ratio)),
        ]),
        Line::from(vec![
            Span::styled("Polymarket滑点 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.1} bps", position.poly_slippage_bps)),
            Span::raw("  "),
            Span::styled("交易所滑点 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.1} bps", position.cex_slippage_bps)),
            Span::raw("  "),
            Span::styled("Polymarket点差 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.2}%", position.poly_spread_pct)),
            Span::raw("  "),
            Span::styled("交易所点差 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.2}%", position.cex_spread_pct)),
        ]),
    ];

    Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 持仓详情 ")
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true })
}

pub fn render_equity_toolbar(state: &TuiState) -> Paragraph<'static> {
    let visible = state.visible_trades().len();
    let total = state.monitor.recent_trades.len();
    let selected = state.selected_trade();
    let lines = vec![
        Line::from(vec![
            Span::styled("时间范围 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                state.equity_range().label(),
                Style::default().fg(Color::Cyan).bold(),
            ),
            Span::raw("  "),
            Span::styled("切换 ", Style::default().fg(Color::DarkGray)),
            Span::raw("←/→"),
            Span::raw("  "),
            Span::styled("区间交易 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{visible}/{total} 笔")),
            Span::raw("  "),
            Span::styled("导航 ", Style::default().fg(Color::DarkGray)),
            Span::raw("↑/↓"),
        ]),
        Line::from(vec![
            Span::styled("当前选中 ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                selected
                    .map(|trade| {
                        format!(
                            "{}  {}  {}",
                            trade.market,
                            trade_buy_sell_pair(trade),
                            format_signed_usd(trade.realized_pnl_usd)
                        )
                    })
                    .unwrap_or_else(|| "暂无交易，底部列表选择后右侧会显示详情".to_owned()),
            ),
        ]),
    ];

    Paragraph::new(lines)
        .block(titled_block(" 净值工具栏 ".to_owned()))
        .wrap(Wrap { trim: true })
}

pub fn render_equity_chart(state: &TuiState) -> Paragraph<'static> {
    let points = realized_equity_points(state);
    let min_equity = points.iter().copied().fold(f64::INFINITY, f64::min);
    let max_equity = points.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let trades = state.visible_trades();

    let chart = sparkline(&points, 56);
    let buy_markers = trade_side_marker_strip(&trades, 56, TradeMarkerSide::Buy);
    let sell_markers = trade_side_marker_strip(&trades, 56, TradeMarkerSide::Sell);
    let lines = vec![
        Line::from(vec![
            Span::styled("回推起点 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "${:.2}",
                points
                    .first()
                    .copied()
                    .unwrap_or(state.monitor.performance.equity)
            )),
            Span::raw("  "),
            Span::styled("范围 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                state.equity_range().label(),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw("  "),
            Span::styled("当前净值 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("${:.2}", state.monitor.performance.equity),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::styled("净值曲线 ", Style::default().fg(Color::DarkGray)),
            Span::styled(chart, Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled("买点标记 ", Style::default().fg(Color::DarkGray)),
            Span::styled(buy_markers, Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled("卖点标记 ", Style::default().fg(Color::DarkGray)),
            Span::styled(sell_markers, Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::styled("区间 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("${:.2} → ${:.2}", min_equity, max_equity)),
            Span::raw("  "),
            Span::styled("交易数 ", Style::default().fg(Color::DarkGray)),
            Span::raw(trades.len().to_string()),
            Span::raw("  "),
            Span::styled("图例 ", Style::default().fg(Color::DarkGray)),
            Span::raw("B=买入  S=卖出  *=重叠"),
        ]),
    ];

    Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 净值曲线 ")
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true })
}

pub fn render_recent_trades(state: &TuiState) -> Table<'static> {
    let trades = state.visible_trades();
    if trades.is_empty() {
        return empty_table(" 区间交易 ", "当前时间范围下暂无已平仓交易");
    }

    let (start, end) = visible_window(trades.len(), state.selected_trade, 10);
    let header = table_header([
        left_cell(""),
        left_cell("时间"),
        left_cell("市场"),
        centered_cell("方向"),
        right_cell("入场"),
        right_cell("出场"),
        right_cell("盈亏"),
    ]);
    let rows = trades
        .iter()
        .enumerate()
        .skip(start)
        .take(end - start)
        .map(|(idx, trade)| {
            let selected = idx == state.selected_trade;
            Row::new([
                left_styled_cell(
                    if selected { "›" } else { " " },
                    Style::default().fg(Color::Cyan),
                ),
                left_styled_cell(
                    fit_display_left(&format_time(trade.closed_at_ms), 11),
                    Style::default().fg(Color::DarkGray),
                ),
                left_cell(truncate_display(&trade.market, 20)),
                centered_cell(trade.direction.clone()),
                right_cell(format_option_price(trade.entry_price)),
                right_cell(format_option_price(trade.exit_price)),
                right_styled_cell(
                    fit_display_right(&format_signed_usd(trade.realized_pnl_usd), 10),
                    Style::default().fg(pnl_color(trade.realized_pnl_usd)),
                ),
            ])
            .style(selected_row_style(selected))
        });

    Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Length(11),
            Constraint::Min(12),
            Constraint::Length(4),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(format!(
        " 区间交易 ({}/{}) / 范围 {} ",
        trades.len(),
        state.monitor.recent_trades.len(),
        state.equity_range().label()
    )))
}

pub fn render_event_log(state: &TuiState) -> Paragraph<'static> {
    let events = state.visible_events();
    if events.is_empty() {
        return Paragraph::new("当前筛选条件下暂无事件")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(active_title(
                        format!(
                            " 事件流 / {} / 未读 {} ",
                            state.event_filter().label(),
                            state.visible_event_unread_count()
                        ),
                        state.page == Page::Log && state.active_log_pane == LogPane::Events,
                    ))
                    .title_style(Style::default().fg(Color::Cyan)),
            )
            .style(Style::default().fg(Color::DarkGray));
    }

    let mut lines = Vec::new();
    let (start, end) = visible_window(events.len(), state.selected_event, 9);
    for (idx, event) in events.iter().enumerate().skip(start).take(end - start) {
        let selected = idx == state.selected_event && state.active_log_pane == LogPane::Events;
        let style = if selected {
            Style::default().bg(Color::DarkGray)
        } else {
            Style::default()
        };
        let (label, color) = event_kind_label(event.kind);
        let unread_marker = if state.is_event_read(event) {
            " "
        } else {
            "●"
        };
        lines.push(Line::from(vec![
            Span::styled(
                if selected { "›" } else { unread_marker },
                style.fg(if unread_marker == "●" {
                    Color::Yellow
                } else {
                    Color::Cyan
                }),
            ),
            Span::styled(format_time(event.timestamp_ms), style.fg(Color::DarkGray)),
            Span::raw(" "),
            Span::styled(format!("[{label}]"), style.fg(color)),
            Span::raw(" "),
            Span::styled(truncate_display(&event.summary, 90), style),
        ]));
    }

    Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(active_title(
                    format!(
                        " 事件流 / {} / 未读 {} ",
                        state.event_filter().label(),
                        state.visible_event_unread_count()
                    ),
                    state.page == Page::Log && state.active_log_pane == LogPane::Events,
                ))
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true })
}

pub fn render_event_details(state: &TuiState) -> Paragraph<'static> {
    let title = active_title(
        " 事件详情 ",
        state.page == Page::Log && state.active_log_pane == LogPane::Events,
    );
    let Some(event) = state.selected_event() else {
        return Paragraph::new("选择一条事件查看详情")
            .block(titled_block(title))
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
    };

    let (label, color) = event_kind_label(event.kind);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("类型 ", Style::default().fg(Color::DarkGray)),
            Span::styled(label, Style::default().fg(color).bold()),
        ]),
        Line::from(vec![
            Span::styled("时间 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_time(event.timestamp_ms)),
        ]),
    ];

    if let Some(market) = event.market.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("市场 ", Style::default().fg(Color::DarkGray)),
            Span::raw(market.to_owned()),
        ]));
    }

    if let Some(correlation_id) = event.correlation_id.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("关联 ", Style::default().fg(Color::DarkGray)),
            Span::raw(correlation_id.to_owned()),
        ]));
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(vec![
        Span::styled("摘要 ", Style::default().fg(Color::DarkGray)),
        Span::raw(event.summary.clone()),
    ]));

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "详情",
        Style::default().fg(Color::Cyan).bold(),
    ));

    match event.details.as_ref() {
        Some(details) if !details.is_empty() => {
            for (key, value) in sorted_detail_items(details) {
                lines.push(Line::from(vec![
                    Span::styled(format!("{key}: "), Style::default().fg(Color::DarkGray)),
                    Span::raw(value),
                ]));
            }
        }
        _ => lines.push(Line::styled(
            "无附加字段",
            Style::default().fg(Color::DarkGray),
        )),
    }

    Paragraph::new(lines)
        .block(titled_block(title))
        .wrap(Wrap { trim: true })
}

pub fn render_log_commands(state: &TuiState) -> Table<'static> {
    let commands = state.visible_commands();
    if commands.is_empty() {
        return empty_table(
            active_title(
                format!(
                    " 最近命令 / {} / 未读 {} ",
                    state.command_filter().label(),
                    state.visible_command_unread_count()
                ),
                state.page == Page::Log && state.active_log_pane == LogPane::Commands,
            ),
            "当前筛选条件下暂无命令",
        );
    }

    let (start, end) = visible_window(commands.len(), state.selected_command, 8);
    let header = table_header([
        left_cell(""),
        left_cell("命令"),
        right_cell("状态"),
        left_cell("消息"),
        centered_cell("可撤"),
    ]);
    let rows = commands
        .iter()
        .enumerate()
        .skip(start)
        .take(end - start)
        .map(|(idx, command)| {
            let selected =
                idx == state.selected_command && state.active_log_pane == LogPane::Commands;
            let status_color = command_status_color(command.status);
            let cancel_hint = if command.cancellable { "x" } else { "" };
            let message = command
                .message
                .as_deref()
                .map(|message| truncate_display(message, 30))
                .unwrap_or_else(|| "-".to_owned());

            Row::new([
                left_styled_cell(
                    if selected {
                        "›"
                    } else if state.is_command_read(command) {
                        " "
                    } else {
                        "●"
                    },
                    Style::default().fg(if selected {
                        Color::Cyan
                    } else if state.is_command_read(command) {
                        Color::DarkGray
                    } else {
                        Color::Yellow
                    }),
                ),
                left_cell(fit_display_left(&format_command_kind(&command.kind), 24)),
                right_styled_cell(
                    fit_display_right(format_command_status(command.status), 14),
                    Style::default().fg(status_color),
                ),
                left_styled_cell(message, Style::default().fg(Color::Gray)),
                right_styled_cell(
                    fit_display_right(cancel_hint, 4),
                    Style::default().fg(Color::Yellow),
                ),
            ])
            .style(selected_row_style(selected))
        });

    Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Length(24),
            Constraint::Length(14),
            Constraint::Min(12),
            Constraint::Length(4),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(titled_block(active_title(
        format!(
            " 最近命令 / {} / 未读 {} ",
            state.command_filter().label(),
            state.visible_command_unread_count()
        ),
        state.page == Page::Log && state.active_log_pane == LogPane::Commands,
    )))
}

pub fn render_log_filters(state: &TuiState) -> Paragraph<'static> {
    let event_line = Line::from(vec![
        Span::styled(
            if state.active_log_pane == LogPane::Events {
                "事件● "
            } else {
                "事件  "
            },
            Style::default().fg(if state.active_log_pane == LogPane::Events {
                Color::Cyan
            } else {
                Color::DarkGray
            }),
        ),
        Span::raw(" "),
        filter_chip(
            state.event_filter().label(),
            state.active_log_pane == LogPane::Events,
        ),
        Span::raw("  "),
        Span::styled("搜索 ", Style::default().fg(Color::DarkGray)),
        search_chip(
            state.event_search_query(),
            state.is_editing_log_search()
                && state.log_search_target() == Some(super::state::LogSearchTarget::Events),
        ),
        Span::raw("  "),
        Span::styled(
            format!(
                "显示 {}/{}  未读 {}  已清 {}",
                state.visible_events().len(),
                state.monitor.recent_events.len(),
                state.visible_event_unread_count(),
                state.cleared_event_count()
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let command_line = Line::from(vec![
        Span::styled(
            if state.active_log_pane == LogPane::Commands {
                "命令● "
            } else {
                "命令  "
            },
            Style::default().fg(if state.active_log_pane == LogPane::Commands {
                Color::Cyan
            } else {
                Color::DarkGray
            }),
        ),
        Span::raw(" "),
        filter_chip(
            state.command_filter().label(),
            state.active_log_pane == LogPane::Commands,
        ),
        Span::raw("  "),
        Span::styled("搜索 ", Style::default().fg(Color::DarkGray)),
        search_chip(
            state.command_search_query(),
            state.is_editing_log_search()
                && state.log_search_target() == Some(super::state::LogSearchTarget::Commands),
        ),
        Span::raw("  "),
        Span::styled(
            format!(
                "显示 {}/{}  未读 {}  已清 {}",
                state.visible_commands().len(),
                state.monitor.recent_commands.len(),
                state.visible_command_unread_count(),
                state.cleared_command_count()
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let ops_line = Line::from(vec![
        Span::styled("操作 ", Style::default().fg(Color::DarkGray)),
        Span::raw("Tab 切窗格  T 切类型  / 搜索  R 标记已读  C 清已读  x 取消命令"),
    ]);

    Paragraph::new(vec![event_line, command_line, ops_line])
        .block(titled_block(" 筛选 ".to_owned()))
        .wrap(Wrap { trim: true })
}

pub fn render_trade_detail(state: &TuiState) -> Paragraph<'static> {
    let Some(trade) = state.selected_trade() else {
        return Paragraph::new("选择一笔区间交易后，这里会显示开平方向、价格、持有时长和关联 ID。")
            .block(titled_block(" 交易详情 ".to_owned()))
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
    };

    let pnl_style = Style::default()
        .fg(pnl_color(trade.realized_pnl_usd))
        .bold();
    let holding_secs = trade.closed_at_ms.saturating_sub(trade.opened_at_ms) / 1000;
    let (open_side, close_side) = trade_buy_sell_sides(trade);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("市场 ", Style::default().fg(Color::DarkGray)),
            Span::raw(trade.market.clone()),
        ]),
        Line::from(vec![
            Span::styled("方向 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} {}",
                display_trade_direction(&trade.direction),
                display_trade_token_side(&trade.token_side)
            )),
        ]),
        Line::from(vec![
            Span::styled("买卖 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{open_side} → {close_side}")),
        ]),
        Line::from(vec![
            Span::styled("开仓 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_time(trade.opened_at_ms)),
        ]),
        Line::from(vec![
            Span::styled("平仓 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_time(trade.closed_at_ms)),
        ]),
        Line::from(vec![
            Span::styled("入场价 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_option_price(trade.entry_price)),
            Span::raw("  "),
            Span::styled("出场价 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_option_price(trade.exit_price)),
        ]),
        Line::from(vec![
            Span::styled("持有时长 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_duration(holding_secs)),
        ]),
        Line::from(vec![
            Span::styled("已实现盈亏 ", Style::default().fg(Color::DarkGray)),
            Span::styled(format_signed_usd(trade.realized_pnl_usd), pnl_style),
        ]),
    ];

    if let Some(correlation_id) = trade.correlation_id.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("关联 ID ", Style::default().fg(Color::DarkGray)),
            Span::raw(correlation_id.to_owned()),
        ]));
    }

    Paragraph::new(lines)
        .block(titled_block(" 交易详情 ".to_owned()))
        .wrap(Wrap { trim: true })
}

pub fn render_command_detail(state: &TuiState) -> Paragraph<'static> {
    let title = active_title(
        " 命令详情 ",
        state.page == Page::Log && state.active_log_pane == LogPane::Commands,
    );
    let Some(command) = state.selected_command() else {
        return Paragraph::new("选择一条命令查看详情")
            .block(titled_block(title))
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
    };

    let status_color = command_status_color(command.status);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("命令 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_command_kind(&command.kind)),
        ]),
        Line::from(vec![
            Span::styled("状态 ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format_command_status(command.status),
                Style::default().fg(status_color).bold(),
            ),
        ]),
        Line::from(vec![
            Span::styled("命令 ID ", Style::default().fg(Color::DarkGray)),
            Span::raw(command.command_id.clone()),
        ]),
        Line::from(vec![
            Span::styled("创建时间 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_time(command.created_at_ms)),
        ]),
        Line::from(vec![
            Span::styled("更新时间 ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_time(command.updated_at_ms)),
        ]),
        Line::from(vec![
            Span::styled("已完成 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if command.finished { "是" } else { "否" }),
            Span::raw("  "),
            Span::styled("超时 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if command.timed_out { "是" } else { "否" }),
            Span::raw("  "),
            Span::styled("可撤销 ", Style::default().fg(Color::DarkGray)),
            Span::raw(if command.cancellable { "是" } else { "否" }),
        ]),
    ];

    match &command.kind {
        CommandKind::ClosePosition { market } => {
            lines.push(Line::from(vec![
                Span::styled("目标市场 ", Style::default().fg(Color::DarkGray)),
                Span::raw(market.clone()),
            ]));
        }
        CommandKind::CancelCommand { command_id } => {
            lines.push(Line::from(vec![
                Span::styled("目标命令 ", Style::default().fg(Color::DarkGray)),
                Span::raw(command_id.clone()),
            ]));
        }
        CommandKind::UpdateConfig { config } => {
            lines.push(Line::from(vec![
                Span::styled("参数变更 ", Style::default().fg(Color::DarkGray)),
                Span::raw(format_config_update_summary(config)),
            ]));
        }
        _ => {}
    }

    if let Some(error_code) = command.error_code {
        lines.push(Line::from(vec![
            Span::styled("错误码 ", Style::default().fg(Color::DarkGray)),
            Span::styled(error_code.to_string(), Style::default().fg(Color::Red)),
        ]));
    }

    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "消息",
        Style::default().fg(Color::Cyan).bold(),
    ));
    lines.push(Line::raw(
        command
            .message
            .clone()
            .unwrap_or_else(|| "无附加消息".to_owned()),
    ));

    Paragraph::new(lines)
        .block(titled_block(title))
        .wrap(Wrap { trim: true })
}

pub fn render_connections(state: &TuiState) -> Paragraph<'static> {
    let mut names = state
        .monitor
        .connections
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    names.sort();

    let mut spans = Vec::new();
    for name in names {
        if let Some(info) = state.monitor.connections.get(&name) {
            if !spans.is_empty() {
                spans.push(Span::raw("  "));
            }
            let age_ms = state.data_age_ms(info.updated_at_ms);
            let transport_idle_ms = info.transport_idle_ms.or(age_ms);
            let (symbol, status_color) = match info.status {
                ConnectionStatus::Connected => ("●", Color::Green),
                ConnectionStatus::Connecting => ("◌", Color::Yellow),
                ConnectionStatus::Reconnecting => ("◐", Color::Yellow),
                ConnectionStatus::Disconnected => ("○", Color::Red),
            };
            let color = if matches!(info.status, ConnectionStatus::Connected) {
                combined_age_color(transport_idle_ms, None)
            } else {
                status_color
            };
            spans.push(Span::styled(
                format!("{name} {symbol}"),
                Style::default().fg(color),
            ));
            if let Some(latency_ms) = info.latency_ms {
                spans.push(Span::styled(
                    format!(" {}ms", latency_ms),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            if let Some(age_ms) = transport_idle_ms {
                spans.push(Span::styled(
                    format!(" 空闲 {}", format_age(age_ms)),
                    Style::default().fg(age_color(age_ms)),
                ));
            }
            if info.reconnect_count > 0 || info.disconnect_count > 0 {
                spans.push(Span::styled(
                    format!(
                        " 重连{} 断开{}",
                        info.reconnect_count, info.disconnect_count
                    ),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            if !matches!(info.status, ConnectionStatus::Connected) {
                if let Some(down_age_ms) = state.data_age_ms(info.last_disconnect_at_ms) {
                    spans.push(Span::styled(
                        format!(" 断开 {}", format_age(down_age_ms)),
                        Style::default().fg(Color::Red),
                    ));
                }
            }
        }
    }

    if spans.is_empty() {
        spans.push(Span::styled(
            "等待连接...",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if state
        .monitor
        .config
        .market_data_mode
        .eq_ignore_ascii_case("ws")
    {
        if !spans.is_empty() {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(
            format!(
                "快照重拉 {} 资金费刷新 {}",
                state.monitor.runtime.snapshot_resync_count,
                state.monitor.runtime.funding_refresh_count
            ),
            Style::default().fg(Color::DarkGray),
        ));
    }

    Paragraph::new(Line::from(spans)).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" 连接 ")
            .title_style(Style::default().fg(Color::Cyan)),
    )
}

pub fn render_help_bar(state: &TuiState) -> Paragraph<'static> {
    let text = match state.page {
        Page::Overview => "1-4 页面  ↑↓ 选择市场  P 暂停/恢复  C 平仓  A 全平  Z/X/S/W 调参  D 导出  ? 帮助  Q 退出",
        Page::Positions => "1-4 页面  ↑↓ 选择持仓  C 平当前  A 全平  Z/X/S/W 调参  D 导出  ? 帮助  Q 退出",
        Page::Equity => "1-4 页面  ←→ 切换时间范围  ↑↓ 选择交易  右侧看详情  D 导出  ? 帮助  Q 退出",
        Page::Log => "1-4 页面  Tab 切窗格  T 切类型  / 搜索  R 标已读  C 清已读  x 取消命令  ? 帮助  Q 退出",
    };
    Paragraph::new(text).style(Style::default().fg(Color::DarkGray))
}

pub fn render_help_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    if !state.show_help {
        return;
    }

    let rect = centered_rect(frame.area(), 112, 24);
    let lines = vec![
        Line::from(Span::styled(
            "监控工作台帮助",
            Style::default().fg(Color::Cyan).bold(),
        )),
        Line::raw(""),
        Line::raw("全局"),
        Line::raw("  1-4 切换页面"),
        Line::raw("  P 暂停 / 恢复"),
        Line::raw("  E 紧急停止 / 解除"),
        Line::raw("  C 在概览 / 持仓页用于平仓；在日志页用于清理已读"),
        Line::raw("  Z 调整入场 Z 值，X 调整出场 Z 值"),
        Line::raw("  S 调整单次仓位，W 调整滚动窗口"),
        Line::raw("  D 导出当前快照；日志页额外导出事件和命令，净值页额外导出交易"),
        Line::raw("  ? 打开 / 关闭帮助，Esc 关闭帮助或弹窗"),
        Line::raw("  Q 退出"),
        Line::raw(""),
        Line::raw("净值页"),
        Line::raw("  ← / → 切换 24小时、3天、7天、全部 四个时间范围"),
        Line::raw("  ↑↓ 在区间交易列表中选择一笔交易，右侧详情联动显示"),
        Line::raw("  图中会标出买点 / 卖点；B=买入，S=卖出，*=重叠"),
        Line::raw(""),
        Line::raw("日志页"),
        Line::raw("  Tab 在事件流 / 最近命令之间切换，右侧显示当前窗格详情"),
        Line::raw("  ↑↓ 选择当前窗格条目"),
        Line::raw("  T 切换当前窗格类型，/ 进入搜索输入"),
        Line::raw("  搜索时 Enter 完成，Esc 退出，Backspace 删除，Delete 清空"),
        Line::raw("  R 标记当前筛选结果为已读，C 清理当前筛选结果中的已读项"),
        Line::raw("  x 取消当前选中的可取消命令"),
        Line::raw(""),
        Line::raw("说明"),
        Line::raw(
            "  界面不做本地乐观状态切换；暂停交易 / 紧急停止 的最终状态以服务端推送状态为准。",
        ),
    ];

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 帮助 ")
                .title_style(Style::default().fg(Color::Cyan)),
        )
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });

    frame.render_widget(Clear, rect);
    frame.render_widget(paragraph, rect);
}

fn render_dialog(state: &TuiState) -> Option<(Paragraph<'static>, Rect)> {
    let dialog = state.dialog.as_ref()?;
    let rect = Rect::new(0, 0, 60, 11);

    let lines = match dialog {
        Dialog::ClosePositionConfirm { market } => vec![
            Line::from(Span::styled(
                "确认平仓",
                Style::default().fg(Color::Yellow).bold(),
            )),
            Line::raw(""),
            Line::raw(format!("市场: {market}")),
            Line::raw("将发送平仓命令。"),
            Line::raw(""),
            Line::raw("[Enter] 确认   [Esc] 取消"),
        ],
        Dialog::CloseAllPositionsConfirm => vec![
            Line::from(Span::styled(
                "确认全部平仓",
                Style::default().fg(Color::Yellow).bold(),
            )),
            Line::raw(""),
            Line::raw(format!("当前持仓数: {}", state.monitor.positions.len())),
            Line::raw("将顺序发送全部平仓命令。"),
            Line::raw(""),
            Line::raw("[Enter] 确认   [Esc] 取消"),
        ],
        Dialog::EmergencyStopConfirm => vec![
            Line::from(Span::styled(
                "确认紧急停止",
                Style::default().fg(Color::Red).bold(),
            )),
            Line::raw(""),
            Line::raw("这会冻结自动行为，并保留现有持仓。"),
            Line::raw(""),
            Line::raw("[Enter] 确认   [Esc] 取消"),
        ],
        Dialog::ClearEmergencyConfirm => vec![
            Line::from(Span::styled(
                "解除紧急停止",
                Style::default().fg(Color::Yellow).bold(),
            )),
            Line::raw(""),
            Line::raw("将发送解除紧急停止命令，实际运行状态以服务端推送为准。"),
            Line::raw(""),
            Line::raw("[Enter] 确认   [Esc] 取消"),
        ],
        Dialog::CancelCommandConfirm {
            command_id,
            summary,
        } => vec![
            Line::from(Span::styled(
                "取消命令",
                Style::default().fg(Color::Yellow).bold(),
            )),
            Line::raw(""),
            Line::raw(format!("目标: {summary}")),
            Line::raw(format!("命令ID: {command_id}")),
            Line::raw(""),
            Line::raw("[Enter] 发送取消命令   [Esc] 取消"),
        ],
        Dialog::AdjustConfig {
            field,
            current,
            draft,
        } => vec![
            Line::from(Span::styled(
                field.title(),
                Style::default().fg(Color::Yellow).bold(),
            )),
            Line::raw(""),
            Line::raw(format!("当前值: {}", field.format_value(*current))),
            Line::raw(format!("新值:   {}", field.format_value(*draft))),
            Line::raw(""),
            Line::raw("[←/h] 减少   [→/l] 增加"),
            Line::raw("[Enter] 发送参数调整   [Esc] 取消"),
        ],
    };

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });

    Some((paragraph, rect))
}

pub fn render_dialog_overlay(frame: &mut ratatui::Frame, state: &TuiState) {
    if let Some((dialog, rect)) = render_dialog(state) {
        let rect = clamp_rect_to_area(
            centered_rect(frame.area(), rect.width, rect.height),
            frame.area(),
        );
        frame.render_widget(Clear, rect);
        frame.render_widget(dialog, rect);
    }
}

fn titled_block(title: impl Into<String>) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .title(title.into())
        .title_style(Style::default().fg(Color::Cyan))
}

fn empty_table(title: impl Into<String>, message: impl Into<String>) -> Table<'static> {
    Table::new(
        [Row::new([left_styled_cell(
            message.into(),
            Style::default().fg(Color::DarkGray),
        )])],
        [Constraint::Min(1)],
    )
    .block(titled_block(title))
}

fn table_header<const N: usize>(cells: [Cell<'static>; N]) -> Row<'static> {
    Row::new(cells).style(Style::default().fg(Color::DarkGray).bold())
}

fn selected_row_style(selected: bool) -> Style {
    if selected {
        Style::default().bg(Color::DarkGray)
    } else {
        Style::default()
    }
}

fn left_cell(value: impl Into<String>) -> Cell<'static> {
    aligned_cell(value, Alignment::Left, Style::default())
}

fn centered_cell(value: impl Into<String>) -> Cell<'static> {
    aligned_cell(value, Alignment::Center, Style::default())
}

fn right_cell(value: impl Into<String>) -> Cell<'static> {
    aligned_cell(value, Alignment::Right, Style::default())
}

fn left_styled_cell(value: impl Into<String>, style: Style) -> Cell<'static> {
    aligned_cell(value, Alignment::Left, style)
}

fn centered_styled_cell(value: impl Into<String>, style: Style) -> Cell<'static> {
    aligned_cell(value, Alignment::Center, style)
}

fn right_styled_cell(value: impl Into<String>, style: Style) -> Cell<'static> {
    aligned_cell(value, Alignment::Right, style)
}

fn aligned_cell(value: impl Into<String>, alignment: Alignment, style: Style) -> Cell<'static> {
    Cell::from(Line::from(value.into()).alignment(alignment)).style(style)
}

fn color_for_banner(level: BannerLevel) -> Color {
    match level {
        BannerLevel::Info => Color::Gray,
        BannerLevel::Success => Color::Green,
        BannerLevel::Warning => Color::Yellow,
        BannerLevel::Error => Color::Red,
    }
}

fn active_title(title: impl AsRef<str>, active: bool) -> String {
    let title = title.as_ref();
    if active {
        format!("{title}●")
    } else {
        title.to_owned()
    }
}

fn filter_chip(label: &str, active: bool) -> Span<'static> {
    let text = format!("[{}]", label);
    if active {
        Span::styled(
            text,
            Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
        )
    } else {
        Span::styled(text, Style::default().fg(Color::DarkGray))
    }
}

fn search_chip(query: &str, active: bool) -> Span<'static> {
    let value = if query.is_empty() { "______" } else { query };
    let text = if active {
        format!("{value}_")
    } else {
        value.to_owned()
    };
    Span::styled(
        text,
        Style::default().fg(if active { Color::Yellow } else { Color::White }),
    )
}

fn format_option_price(price: Option<f64>) -> String {
    price
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn sorted_markets(markets: &[MarketView]) -> Vec<&MarketView> {
    let mut items = markets.iter().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        market_tier_priority(right)
            .cmp(&market_tier_priority(left))
            .then_with(|| market_availability_rank(right).cmp(&market_availability_rank(left)))
            .then_with(|| {
                market_rank(right)
                    .partial_cmp(&market_rank(left))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| right.has_position.cmp(&left.has_position))
            .then_with(|| newest_market_sample_ms(right).cmp(&newest_market_sample_ms(left)))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    items
}

fn market_tier_priority(market: &MarketView) -> i32 {
    match market.market_tier {
        MarketTier::Tradeable => 400,
        MarketTier::Focus => 200 + retention_reason_priority(market.retention_reason),
        MarketTier::Observation => 100,
    }
}

fn retention_reason_priority(reason: MarketRetentionReason) -> i32 {
    match reason {
        MarketRetentionReason::None => 0,
        MarketRetentionReason::HasPosition => 2,
        MarketRetentionReason::CloseInProgress => 3,
        MarketRetentionReason::DeltaRebalance => 3,
        MarketRetentionReason::ResidualRecovery => 4,
        MarketRetentionReason::ForceExit => 5,
    }
}

fn market_availability_rank(market: &MarketView) -> i32 {
    market_health_rank(market)
}

fn newest_market_sample_ms(market: &MarketView) -> u64 {
    market
        .poly_updated_at_ms
        .unwrap_or_default()
        .max(market.cex_updated_at_ms.unwrap_or_default())
}

fn market_rank(market: &MarketView) -> f64 {
    market
        .fair_value
        .or_else(|| market.z_score.map(|value| value.abs()))
        .or_else(|| market.basis_pct.map(|value| value.abs()))
        .unwrap_or(-1.0)
}

fn position_color(side: &str) -> Color {
    if side.contains("LONG") {
        Color::Green
    } else if side.contains("SHORT") {
        Color::Red
    } else {
        Color::Gray
    }
}

fn short_position_side(side: &str) -> &str {
    if side.contains("LONG") {
        "多"
    } else if side.contains("SHORT") {
        "空"
    } else {
        "--"
    }
}

fn event_kind_label(kind: EventKind) -> (&'static str, Color) {
    match kind {
        EventKind::Signal => ("信号", Color::Yellow),
        EventKind::Gate => ("闸门", Color::Magenta),
        EventKind::Trade => ("成交", Color::Green),
        EventKind::TradeClosed => ("平仓", Color::Cyan),
        EventKind::Skip => ("跳过", Color::Cyan),
        EventKind::Risk => ("风控", Color::Red),
        EventKind::System => ("系统", Color::Blue),
        EventKind::Error => ("错误", Color::Red),
    }
}

fn market_data_mode_badge(mode: &str) -> (&'static str, Color) {
    if mode.eq_ignore_ascii_case("ws") {
        ("WS（推送）", Color::Green)
    } else {
        ("POLL（轮询）", Color::Yellow)
    }
}

fn display_position_side(side: &str) -> &'static str {
    if side.contains("LONG") {
        "多"
    } else if side.contains("SHORT") {
        "空"
    } else {
        "平"
    }
}

fn display_poly_side(side: &str) -> String {
    let direction = display_position_side(side);
    let token = if side.contains("YES") {
        " YES"
    } else if side.contains("NO") {
        " NO"
    } else {
        ""
    };
    format!("{direction}{token}")
}

fn display_trade_direction(direction: &str) -> &'static str {
    if direction.contains("SHORT") || direction.contains("空") {
        "做空"
    } else if direction.contains("LONG") || direction.contains("多") {
        "做多"
    } else {
        "未知"
    }
}

fn display_trade_token_side(token_side: &str) -> &'static str {
    if token_side.contains("YES") {
        "YES"
    } else if token_side.contains("NO") {
        "NO"
    } else {
        "--"
    }
}

fn trade_buy_sell_sides(trade: &TradeView) -> (&'static str, &'static str) {
    if trade.direction.contains("SHORT") || trade.direction.contains("空") {
        ("卖", "买")
    } else {
        ("买", "卖")
    }
}

fn trade_buy_sell_pair(trade: &TradeView) -> String {
    let (open_side, close_side) = trade_buy_sell_sides(trade);
    format!("{open_side}→{close_side}")
}

fn format_optional_price(price: Option<f64>) -> String {
    price.map(format_price).unwrap_or_else(|| "--".to_owned())
}

fn format_market_age(poly_age_ms: Option<u64>, cex_age_ms: Option<u64>) -> String {
    match (poly_age_ms, cex_age_ms) {
        (Some(poly), Some(cex)) => format!("{}/{}", format_age(poly), format_age(cex)),
        (Some(poly), None) => format!("{}/--", format_age(poly)),
        (None, Some(cex)) => format!("--/{}", format_age(cex)),
        (None, None) => "--".to_owned(),
    }
}

fn format_optional_age(age_ms: Option<u64>) -> String {
    age_ms.map(format_age).unwrap_or_else(|| "--".to_owned())
}

fn format_optional_metric(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "--".to_owned())
}

fn format_entry_target_pair(entry_price: Option<f64>, target_exit_price: Option<f64>) -> String {
    match (entry_price, target_exit_price) {
        (Some(entry), Some(target)) => format!("{}/{}", format_price(entry), format_price(target)),
        _ => "--".to_owned(),
    }
}

fn format_pocket_summary(ticks: Option<f64>, notional_usd: Option<f64>) -> String {
    match (ticks, notional_usd) {
        (Some(ticks), Some(notional_usd)) => format!("{ticks:.0}/${notional_usd:.0}"),
        (Some(ticks), None) => format!("{ticks:.0}/--"),
        (None, Some(notional_usd)) => format!("--/${notional_usd:.0}"),
        (None, None) => "--".to_owned(),
    }
}

fn format_optional_pct(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}%"))
        .unwrap_or_else(|| "--".to_owned())
}

fn format_optional_ratio_pct(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "--".to_owned())
}

fn format_price(price: f64) -> String {
    if price <= 0.0 {
        "--".to_owned()
    } else if price < 1.0 {
        format!("{price:.3}")
    } else if price < 100.0 {
        format!("{price:.2}")
    } else {
        format!("{price:.0}")
    }
}

fn format_age(age_ms: u64) -> String {
    if age_ms < 10_000 {
        format!("{:.1}s", age_ms as f64 / 1000.0)
    } else if age_ms < 60_000 {
        format!("{}s", age_ms / 1000)
    } else if age_ms < 3_600_000 {
        format!("{}m", age_ms / 60_000)
    } else {
        format!("{}h", age_ms / 3_600_000)
    }
}

fn age_color(age_ms: u64) -> Color {
    if age_ms > 15_000 {
        Color::Red
    } else if age_ms > 5_000 {
        Color::Yellow
    } else {
        Color::DarkGray
    }
}

fn combined_age_color(left_age_ms: Option<u64>, right_age_ms: Option<u64>) -> Color {
    match (left_age_ms, right_age_ms) {
        (Some(left), Some(right)) => age_color(left.max(right)),
        (Some(age), None) | (None, Some(age)) => age_color(age),
        (None, None) => Color::DarkGray,
    }
}

fn format_signed_usd(value: f64) -> String {
    if value >= 0.0 {
        format!("+${:.2}", value.abs())
    } else {
        format!("-${:.2}", value.abs())
    }
}

fn format_optional_signed_usd(value: Option<f64>) -> String {
    value
        .map(format_signed_usd)
        .unwrap_or_else(|| "--".to_owned())
}

fn format_optional_cost_usd(value: Option<f64>) -> String {
    value
        .map(|value| format!("${:.2}", value.abs()))
        .unwrap_or_else(|| "--".to_owned())
}

fn top_count_key(values: &HashMap<String, u64>) -> Option<(&str, u64)> {
    values
        .iter()
        .max_by(|left, right| {
            left.1
                .cmp(right.1)
                .then_with(|| left.0.cmp(right.0).reverse())
        })
        .map(|(reason, count)| (reason.as_str(), *count))
}

fn sorted_detail_items(details: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut items = details
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    items.sort_by(|(left_key, left_value), (right_key, right_value)| {
        detail_rank(left_key)
            .cmp(&detail_rank(right_key))
            .then_with(|| left_key.cmp(right_key))
            .then_with(|| left_value.cmp(right_value))
    });
    items
}

fn detail_rank(key: &str) -> usize {
    match key {
        "市场" => 0,
        "触发ID" => 1,
        "信号ID" => 1,
        "关联ID" => 2,
        "计划ID" => 3,
        "恢复计划ID" => 4,
        "父计划ID" => 5,
        "替换前计划ID" => 6,
        "计划哈希" => 7,
        "幂等键" => 8,
        "Schema版本" => 9,
        "意图" => 10,
        "意图类型" => 11,
        "优先级" => 12,
        "闸门" => 13,
        "闸门结果" => 14,
        "计划拒绝代码" => 15,
        "原因" => 16,
        "动作" => 17,
        "强度" => 18,
        "方向" => 19,
        "Token方向" => 20,
        "平仓原因" => 21,
        "公允值" => 22,
        "原始错价" => 23,
        "Delta估计" => 24,
        "预算上限USD" => 25,
        "残余Delta阈值" => 26,
        "冲击亏损阈值USD" => 27,
        "冲击亏损阈值" => 28,
        "平仓比例" => 29,
        "残余快照" => 30,
        "恢复原因" => 31,
        "强退原因" => 32,
        "允许负边际" => 33,
        "计划毛EdgeUSD" => 34,
        "计划费用USD" => 35,
        "计划净EdgeUSD" => 36,
        "Poly定价模式" => 37,
        "Poly请求股数" => 38,
        "Poly计划股数" => 39,
        "Poly预算上限USD" => 40,
        "Poly最大均价" => 41,
        "Poly最小均价" => 42,
        "Poly最小收入USD" => 43,
        "Poly簿面均价" => 44,
        "Poly可成交价" => 45,
        "Poly吃簿成本USD" => 46,
        "Poly手续费USD" => 47,
        "CEX计划数量" => 48,
        "CEX簿面均价" => 49,
        "CEX可成交价" => 50,
        "CEX摩擦成本USD" => 51,
        "CEX手续费USD" => 52,
        "理论边际USD" => 53,
        "计划边际USD" => 54,
        "Funding成本USD" => 55,
        "残余风险惩罚USD" => 56,
        "计划残余Delta" => 57,
        "计划冲击亏损+1%USD" => 58,
        "计划冲击亏损-1%USD" => 59,
        "计划冲击亏损+2%USD" => 60,
        "计划冲击亏损-2%USD" => 61,
        "计划TTL" => 62,
        "结果状态" => 63,
        "实际Poly成本USD" => 64,
        "实际CEX成本USD" => 65,
        "实际Poly手续费USD" => 66,
        "实际CEX手续费USD" => 67,
        "实际Funding成本USD" => 68,
        "实际边际USD" => 69,
        "计划偏差USD" => 70,
        "实际残余Delta" => 71,
        "实际冲击亏损+1%USD" => 72,
        "实际冲击亏损-1%USD" => 73,
        "实际冲击亏损+2%USD" => 74,
        "实际冲击亏损-2%USD" => 75,
        "需要恢复" => 76,
        "Poly账本条数" => 77,
        "CEX账本条数" => 78,
        "结果时间" => 79,
        "市场阶段" => 80,
        "评估状态" => 81,
        "异步分类" => 82,
        "Polymarket YES" => 83,
        "Polymarket NO" => 84,
        "CEX中价" => 85,
        "过滤价格" => 86,
        "价格窗口" => 87,
        "Polymarket更新时间" => 88,
        "CEX更新时间" => 89,
        "Polymarket报价龄" => 90,
        "CEX报价龄" => 91,
        "双腿时差" => 92,
        "Poly序列" => 93,
        "Poly交易所时间" => 94,
        "Poly接收时间" => 95,
        "CEX序列" => 96,
        "CEX交易所时间" => 97,
        "CEX接收时间" => 98,
        "连接状态" => 99,
        "连接空闲" => 100,
        "当前总敞口" => 101,
        "当前单市场敞口" => 102,
        "当日已实现盈亏" => 103,
        "熔断器" => 104,
        "熔断原因" => 105,
        "总敞口上限" => 106,
        "单市场上限" => 107,
        "日亏损上限" => 108,
        "交易所" => 109,
        "订单状态" => 110,
        "订单ID" => 111,
        "成交ID" => 112,
        "价格" => 113,
        "数量" => 114,
        "名义" => 115,
        "手续费" => 116,
        "已实现盈亏" => 117,
        "时间" => 118,
        "更新时间" => 119,
        "空闲时长" => 120,
        "重连次数" => 121,
        "断开次数" => 122,
        "最近断开" => 123,
        "影响市场数" => 124,
        _ => 1000,
    }
}

fn skip_reason_label(reason: &str) -> &'static str {
    match reason {
        "no_fresh_poly_sample" => "暂无新的 Polymarket 行情样本",
        "poly_price_stale" => "Polymarket 行情已超时",
        "cex_price_stale" => "交易所行情已超时",
        "data_misaligned" => "Polymarket 与交易所时间未对齐",
        "connection_lost" => "行情连接异常",
        "no_cex_reference" => "缺少可用的交易所参考价",
        _ => "未知跳过原因",
    }
}

fn evaluable_status_label(status: &str) -> &'static str {
    match status {
        "evaluable" => "可定价",
        "not_evaluable_no_poly" => "缺少新的 Polymarket 样本",
        "not_evaluable_no_cex" => "缺少可用的交易所参考价",
        "not_evaluable_misaligned" => "双腿时间未对齐",
        "connection_impaired" => "行情连接异常",
        "poly_quote_stale" => "Polymarket 报价已超时",
        "cex_quote_stale" => "交易所报价已超时",
        _ => "未知状态",
    }
}

fn async_classification_label(classification: &str) -> &'static str {
    match classification {
        "balanced_fresh" => "双腿新鲜",
        "poly_lag_acceptable" => "慢腿可交易",
        "poly_lag_borderline" => "慢腿临界",
        "no_poly_sample" => "缺少新的 Polymarket 样本",
        "no_cex_reference" => "缺少可用的交易所参考价",
        "poly_quote_stale" => "Polymarket 报价已超时",
        "cex_quote_stale" => "交易所报价已超时",
        "misaligned" => "双腿时间未对齐",
        "connection_impaired" => "行情连接异常",
        _ => "未知异步",
    }
}

fn compact_evaluable_status_label(status: EvaluableStatus) -> &'static str {
    match status {
        EvaluableStatus::Unknown => "未知",
        EvaluableStatus::Evaluable => "可定价",
        EvaluableStatus::NotEvaluableNoPoly => "Poly无样本",
        EvaluableStatus::NotEvaluableNoCex => "交易所无价",
        EvaluableStatus::NotEvaluableMisaligned => "双腿未对齐",
        EvaluableStatus::ConnectionImpaired => "连接异常",
        EvaluableStatus::PolyQuoteStale => "Poly超时",
        EvaluableStatus::CexQuoteStale => "交易所超时",
    }
}

fn compact_async_classification_label(classification: AsyncClassification) -> &'static str {
    match classification {
        AsyncClassification::Unknown => "未知",
        AsyncClassification::BalancedFresh => "双腿新鲜",
        AsyncClassification::PolyLagAcceptable => "慢腿可交",
        AsyncClassification::PolyLagBorderline => "慢腿临界",
        AsyncClassification::NoPolySample => "Poly 无新样本",
        AsyncClassification::NoCexReference => "交易所无参考价",
        AsyncClassification::PolyQuoteStale => "Poly 报价超时",
        AsyncClassification::CexQuoteStale => "交易所报价超时",
        AsyncClassification::Misaligned => "双腿未对齐",
        AsyncClassification::ConnectionImpaired => "连接异常",
    }
}

fn market_health_rank(market: &MarketView) -> i32 {
    match market.evaluable_status {
        EvaluableStatus::Evaluable => match market.async_classification {
            AsyncClassification::BalancedFresh => 6,
            AsyncClassification::PolyLagAcceptable => 5,
            AsyncClassification::PolyLagBorderline => 4,
            _ => 3,
        },
        EvaluableStatus::Unknown => {
            if newest_market_sample_ms(market) > 0 {
                2
            } else {
                1
            }
        }
        EvaluableStatus::ConnectionImpaired => 0,
        EvaluableStatus::NotEvaluableNoPoly
        | EvaluableStatus::NotEvaluableNoCex
        | EvaluableStatus::NotEvaluableMisaligned
        | EvaluableStatus::PolyQuoteStale
        | EvaluableStatus::CexQuoteStale => -1,
    }
}

fn market_has_insufficient_warmup(market: &MarketView, min_warmup_samples: usize) -> bool {
    min_warmup_samples > 0
        && market.z_score.is_none()
        && market.fair_value.is_some()
        && market.basis_history_len > 0
        && market.basis_history_len < min_warmup_samples
}

fn compact_market_condition_label(market: &MarketView, min_warmup_samples: usize) -> String {
    if market_has_insufficient_warmup(market, min_warmup_samples) {
        return format!("预热{}/{}", market.basis_history_len, min_warmup_samples);
    }
    if let Some(label) = compact_pricing_warmup_label(market) {
        return label;
    }

    match market.evaluable_status {
        EvaluableStatus::Evaluable => match market.async_classification {
            AsyncClassification::Unknown => {
                compact_evaluable_status_label(market.evaluable_status).to_owned()
            }
            classification => compact_async_classification_label(classification).to_owned(),
        },
        status => compact_evaluable_status_label(status).to_owned(),
    }
}

fn compact_pricing_warmup_label(market: &MarketView) -> Option<String> {
    let source = market.sigma_source.as_deref()?;
    if source.eq_ignore_ascii_case("realized") || market.returns_window_len == 0 {
        return None;
    }
    Some(format!("估值预热{}笔", market.returns_window_len))
}

fn format_market_status_compact(market: &MarketView, min_warmup_samples: usize) -> String {
    let tier = market.market_tier.label_zh();
    if market.retention_reason != MarketRetentionReason::None {
        format!("{}/{}", tier, market.retention_reason.compact_label_zh())
    } else {
        format!(
            "{}/{}",
            tier,
            compact_market_condition_label(market, min_warmup_samples)
        )
    }
}

fn market_status_color(market: &MarketView, min_warmup_samples: usize) -> Color {
    if market_has_insufficient_warmup(market, min_warmup_samples) {
        return Color::Yellow;
    }
    if compact_pricing_warmup_label(market).is_some() {
        return Color::Yellow;
    }

    match market.market_tier {
        MarketTier::Tradeable => match market.async_classification {
            AsyncClassification::BalancedFresh => Color::Green,
            AsyncClassification::PolyLagAcceptable => Color::Cyan,
            _ => Color::Green,
        },
        MarketTier::Focus => match market.retention_reason {
            MarketRetentionReason::ForceExit => Color::Red,
            MarketRetentionReason::ResidualRecovery
            | MarketRetentionReason::CloseInProgress
            | MarketRetentionReason::DeltaRebalance => Color::Yellow,
            MarketRetentionReason::HasPosition | MarketRetentionReason::None => Color::Cyan,
        },
        MarketTier::Observation => match market.evaluable_status {
            EvaluableStatus::ConnectionImpaired => Color::Yellow,
            EvaluableStatus::Unknown => Color::DarkGray,
            _ => Color::Gray,
        },
    }
}

fn skip_rate_color(skip_rate: f64) -> Color {
    if skip_rate >= 80.0 {
        Color::Red
    } else if skip_rate >= 40.0 {
        Color::Yellow
    } else {
        Color::Gray
    }
}

fn pnl_color(value: f64) -> Color {
    if value >= 0.0 {
        Color::Green
    } else {
        Color::Red
    }
}

fn command_status_color(status: CommandStatus) -> Color {
    match status {
        CommandStatus::Pending => Color::Yellow,
        CommandStatus::Running => Color::Blue,
        CommandStatus::Success => Color::Green,
        CommandStatus::Failed => Color::Red,
        CommandStatus::PartialSuccess => Color::LightYellow,
    }
}

pub(super) fn format_command_kind(kind: &CommandKind) -> String {
    match kind {
        CommandKind::Pause => "暂停交易".to_owned(),
        CommandKind::Resume => "恢复交易".to_owned(),
        CommandKind::ClosePosition { market } => format!("平仓({market})"),
        CommandKind::CloseAllPositions => "全部平仓".to_owned(),
        CommandKind::EmergencyStop => "紧急停止".to_owned(),
        CommandKind::ClearEmergency => "解除紧急停止".to_owned(),
        CommandKind::UpdateConfig { config } => {
            format!("调整参数({})", format_config_update_summary(config))
        }
        CommandKind::CancelCommand { command_id } => format!("取消命令({command_id})"),
    }
}

fn format_config_update_summary(config: &polyalpha_core::ConfigUpdate) -> String {
    if let Some(value) = config.entry_z {
        format!("入场Z={value:.2}")
    } else if let Some(value) = config.exit_z {
        format!("出场Z={value:.2}")
    } else if let Some(value) = config.position_notional_usd {
        format!("单次仓位=${value:.0}")
    } else if let Some(value) = config.rolling_window {
        format!("滚动窗口={}s", value)
    } else if let Some(value) = config.band_policy.as_deref() {
        format!("价格带策略={value}")
    } else if let Some(value) = config.min_poly_price {
        format!("最小Polymarket价格={value:.3}")
    } else if let Some(value) = config.max_poly_price {
        format!("最大Polymarket价格={value:.3}")
    } else {
        "未指定参数".to_owned()
    }
}

pub(super) fn format_command_status(status: CommandStatus) -> &'static str {
    match status {
        CommandStatus::Pending => "待处理",
        CommandStatus::Running => "执行中",
        CommandStatus::Success => "成功",
        CommandStatus::Failed => "失败",
        CommandStatus::PartialSuccess => "部分成功",
    }
}

fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h{minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_time(timestamp_ms: u64) -> String {
    let Some(datetime) = Local.timestamp_millis_opt(timestamp_ms as i64).single() else {
        return "--:--:--".to_owned();
    };
    let now = Local::now();
    if datetime.date_naive() == now.date_naive() {
        datetime.format("%H:%M:%S").to_string()
    } else {
        datetime.format("%m-%d %H:%M").to_string()
    }
}

fn visible_window(len: usize, selected: usize, window: usize) -> (usize, usize) {
    if len <= window {
        return (0, len);
    }
    let half = window / 2;
    let mut start = selected.saturating_sub(half);
    if start + window > len {
        start = len - window;
    }
    (start, start + window)
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn truncate_display(value: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }

    let mut output = String::new();
    let mut width = 0usize;
    for ch in value.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + ch_width > max_width {
            break;
        }
        output.push(ch);
        width += ch_width;
    }
    output
}

fn pad_display_right(value: &str, target_width: usize) -> String {
    let width = display_width(value);
    if width >= target_width {
        return value.to_owned();
    }

    format!("{value}{}", " ".repeat(target_width - width))
}

fn pad_display_left(value: &str, target_width: usize) -> String {
    let width = display_width(value);
    if width >= target_width {
        return value.to_owned();
    }

    format!("{}{value}", " ".repeat(target_width - width))
}

fn fit_display_left(value: &str, target_width: usize) -> String {
    pad_display_right(&truncate_display(value, target_width), target_width)
}

fn fit_display_right(value: &str, target_width: usize) -> String {
    pad_display_left(&truncate_display(value, target_width), target_width)
}

fn realized_equity_points(state: &TuiState) -> Vec<f64> {
    let mut trades = state
        .visible_trades()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    trades.sort_by_key(|trade| trade.closed_at_ms);

    if trades.is_empty() {
        return vec![state.monitor.performance.equity];
    }

    let window_realized = trades
        .iter()
        .map(|trade| trade.realized_pnl_usd)
        .sum::<f64>();
    let mut equity = state.monitor.performance.equity - window_realized;
    let mut points = vec![equity];
    for trade in trades {
        equity += trade.realized_pnl_usd;
        points.push(equity);
    }
    if points
        .last()
        .is_none_or(|last| (*last - state.monitor.performance.equity).abs() > f64::EPSILON)
    {
        points.push(state.monitor.performance.equity);
    }
    points
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TradeMarkerSide {
    Buy,
    Sell,
}

fn trade_side_marker_strip(
    trades: &[&polyalpha_core::TradeView],
    width: usize,
    side: TradeMarkerSide,
) -> String {
    if width == 0 {
        return String::new();
    }
    if trades.is_empty() {
        return " ".repeat(width);
    }

    let timestamps = trades
        .iter()
        .flat_map(|trade| [trade.opened_at_ms, trade.closed_at_ms])
        .collect::<Vec<_>>();
    let min_ts = timestamps.iter().copied().min().unwrap_or_default();
    let max_ts = timestamps.iter().copied().max().unwrap_or(min_ts);
    let span = max_ts.saturating_sub(min_ts).max(1);
    let mut slots = vec![' '; width];

    for trade in trades {
        for (ts, marker_side) in trade_marker_points(trade) {
            if marker_side != side {
                continue;
            }
            let idx = (((ts.saturating_sub(min_ts)) as f64 / span as f64)
                * (width.saturating_sub(1) as f64))
                .round() as usize;
            let slot = &mut slots[idx.min(width.saturating_sub(1))];
            let marker = match marker_side {
                TradeMarkerSide::Buy => 'B',
                TradeMarkerSide::Sell => 'S',
            };
            *slot = if *slot == ' ' || *slot == marker {
                marker
            } else {
                '*'
            };
        }
    }

    slots.into_iter().collect()
}

fn trade_marker_points(trade: &polyalpha_core::TradeView) -> [(u64, TradeMarkerSide); 2] {
    if trade.direction.contains("SHORT") || trade.direction.contains("空") {
        [
            (trade.opened_at_ms, TradeMarkerSide::Sell),
            (trade.closed_at_ms, TradeMarkerSide::Buy),
        ]
    } else {
        [
            (trade.opened_at_ms, TradeMarkerSide::Buy),
            (trade.closed_at_ms, TradeMarkerSide::Sell),
        ]
    }
}

fn sparkline(values: &[f64], width: usize) -> String {
    const CHARS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if values.is_empty() || width == 0 {
        return String::new();
    }

    let sampled = if values.len() <= width {
        values.to_vec()
    } else {
        (0..width)
            .map(|idx| {
                let source = idx * (values.len() - 1) / (width - 1).max(1);
                values[source]
            })
            .collect::<Vec<_>>()
    };

    let min = sampled.iter().copied().fold(f64::INFINITY, f64::min);
    let max = sampled.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let span = (max - min).max(f64::EPSILON);

    sampled
        .into_iter()
        .map(|value| {
            let rank = (((value - min) / span) * (CHARS.len() as f64 - 1.0)).round() as usize;
            CHARS[rank.min(CHARS.len() - 1)]
        })
        .collect()
}

fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width.saturating_sub(2).max(1));
    let height = height.min(area.height.saturating_sub(2).max(1));
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(area.height.saturating_sub(height) / 2),
            Constraint::Length(height),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(area.width.saturating_sub(width) / 2),
            Constraint::Length(width),
            Constraint::Min(0),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn clamp_rect_to_area(rect: Rect, area: Rect) -> Rect {
    let x = rect.x.min(area.width.saturating_sub(1));
    let y = rect.y.min(area.height.saturating_sub(1));
    let width = rect.width.min(area.width.saturating_sub(x)).max(1);
    let height = rect.height.min(area.height.saturating_sub(y)).max(1);
    Rect::new(x, y, width, height)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_core::{
        CommandView, EventKind, MonitorEvent, PositionView, PulseAssetHealthRow,
        PulseMarketMonitorRow, PulseMonitorView, PulseSessionMonitorRow,
    };
    use ratatui::{buffer::Buffer, widgets::Widget};

    #[test]
    fn display_width_helpers_handle_cjk_strings() {
        assert_eq!(display_width("ABC"), 3);
        assert_eq!(display_width("中文"), 4);
        assert_eq!(truncate_display("中文ABC", 5), "中文A");
        assert_eq!(pad_display_right("中文", 6), "中文  ");
        assert_eq!(pad_display_left("中文", 6), "  中文");
        assert_eq!(fit_display_left("中文ABC", 6), "中文AB");
        assert_eq!(fit_display_right("中文A", 6), " 中文A");
    }

    #[test]
    fn render_markets_keeps_numeric_columns_aligned_with_cjk_symbols() {
        let mut state = TuiState::default();
        state.monitor.markets = vec![
            sample_market("中文市场测试", Some(7.89)),
            sample_market("ASCII-MARKET", Some(7.89)),
        ];

        let mut buffer = Buffer::empty(Rect::new(0, 0, 90, 6));
        render_markets(&state).render(buffer.area, &mut buffer);

        let first = row_symbols(&buffer, 2);
        let second = row_symbols(&buffer, 3);
        let expected = ["7", ".", "8", "9"];

        assert_eq!(
            find_sequence(&first, &expected),
            find_sequence(&second, &expected)
        );
    }

    #[test]
    fn render_markets_shows_pulse_market_rows_when_present() {
        let mut state = TuiState::default();
        state.monitor.pulse = Some(PulseMonitorView {
            active_sessions: Vec::new(),
            asset_health: vec![PulseAssetHealthRow {
                asset: "btc".to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(15),
                cex_quote_age_ms: Some(8),
                open_sessions: 1,
                net_target_delta: Some(0.41),
                actual_exchange_position: Some(0.39),
                status: Some("enabled".to_owned()),
                disable_reason: None,
            }],
            markets: vec![PulseMarketMonitorRow {
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
                open_sessions: 0,
                status: "ready".to_owned(),
                disable_reason: None,
            }],
            selected_session: None,
        });

        let mut buffer = Buffer::empty(Rect::new(0, 0, 140, 6));
        render_markets(&state).render(buffer.area, &mut buffer);

        let compact = compact_text(&buffer_text(&buffer));
        assert!(compact.contains("btc-above-100k"));
        assert!(compact.contains("NO"));
        assert!(compact.contains("182.5"));
        assert!(compact.contains("97.4"));
        assert!(compact.contains("3.85"));
        assert!(compact.contains("0.356/0.380"));
        assert!(compact.contains("4/$29"));
        assert!(compact.contains("可定价"));
    }

    #[test]
    fn market_table_uses_compact_status_labels() {
        let cases = [
            (
                MarketTier::Tradeable,
                MarketRetentionReason::None,
                EvaluableStatus::Evaluable,
                AsyncClassification::BalancedFresh,
                "交易池/双腿新鲜",
            ),
            (
                MarketTier::Tradeable,
                MarketRetentionReason::None,
                EvaluableStatus::Evaluable,
                AsyncClassification::PolyLagAcceptable,
                "交易池/慢腿可交",
            ),
            (
                MarketTier::Focus,
                MarketRetentionReason::None,
                EvaluableStatus::Evaluable,
                AsyncClassification::PolyLagBorderline,
                "重点池/慢腿临界",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::NotEvaluableNoPoly,
                AsyncClassification::NoPolySample,
                "观察池/Poly无样本",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::NotEvaluableNoCex,
                AsyncClassification::NoCexReference,
                "观察池/交易所无价",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::NotEvaluableMisaligned,
                AsyncClassification::Misaligned,
                "观察池/双腿未对齐",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::ConnectionImpaired,
                AsyncClassification::ConnectionImpaired,
                "观察池/连接异常",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::PolyQuoteStale,
                AsyncClassification::PolyQuoteStale,
                "观察池/Poly超时",
            ),
            (
                MarketTier::Observation,
                MarketRetentionReason::None,
                EvaluableStatus::CexQuoteStale,
                AsyncClassification::CexQuoteStale,
                "观察池/交易所超时",
            ),
            (
                MarketTier::Focus,
                MarketRetentionReason::HasPosition,
                EvaluableStatus::Evaluable,
                AsyncClassification::BalancedFresh,
                "重点池/持仓",
            ),
        ];

        for (market_tier, retention_reason, evaluable_status, async_classification, expected) in
            cases
        {
            let mut market = sample_market("xrp-dip-120-mar", Some(-1.22));
            market.market_tier = market_tier;
            market.retention_reason = retention_reason;
            market.evaluable_status = evaluable_status;
            market.async_classification = async_classification;

            let actual = format_market_status_compact(&market, 600);
            assert_eq!(actual, expected);
            assert!(display_width(&actual) <= 18, "{actual}");
        }
    }

    #[test]
    fn sorted_markets_prioritize_tradeable_then_retained_focus_then_observation() {
        let mut tradeable = sample_market("tradeable-market", Some(1.0));
        tradeable.market_tier = MarketTier::Tradeable;
        tradeable.retention_reason = MarketRetentionReason::None;

        let mut focus = sample_market("focus-market", Some(9.0));
        focus.market_tier = MarketTier::Focus;
        focus.retention_reason = MarketRetentionReason::HasPosition;

        let mut observation = sample_market("observation-market", Some(20.0));
        observation.market_tier = MarketTier::Observation;
        observation.retention_reason = MarketRetentionReason::None;

        let markets = vec![observation, focus, tradeable];
        let sorted = sorted_markets(&markets);
        let symbols = sorted
            .into_iter()
            .map(|market| market.symbol.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            symbols,
            vec![
                "tradeable-market".to_owned(),
                "focus-market".to_owned(),
                "observation-market".to_owned(),
            ]
        );
    }

    #[test]
    fn render_log_commands_keeps_status_column_aligned_with_cjk_command_kinds() {
        let mut state = TuiState::default();
        state.page = Page::Log;
        state.active_log_pane = LogPane::Commands;
        state.monitor.recent_commands = vec![
            sample_command("中文市场测试"),
            sample_command("ASCII-MARKET"),
        ];

        let mut buffer = Buffer::empty(Rect::new(0, 0, 90, 6));
        render_log_commands(&state).render(buffer.area, &mut buffer);

        let first = row_symbols(&buffer, 2);
        let second = row_symbols(&buffer, 3);
        let expected = ["x"];

        assert_eq!(
            find_sequence(&first, &expected),
            find_sequence(&second, &expected)
        );
    }

    #[test]
    fn render_signals_shows_chinese_skip_summary_and_badge() {
        let mut state = TuiState::default();
        state.monitor.evaluation.attempts = 171;
        state.monitor.evaluation.skipped = 50;
        state
            .monitor
            .evaluation
            .skip_reasons
            .insert("no_fresh_poly_sample".to_owned(), 50);
        state.monitor.signals.seen = 3;
        state.monitor.recent_events = vec![MonitorEvent {
            schema_version: polyalpha_core::PLANNING_SCHEMA_VERSION,
            timestamp_ms: 1_774_362_516_166,
            kind: EventKind::Skip,
            market: Some("xrp-dip-120-mar".to_owned()),
            correlation_id: None,
            summary: "xrp-dip-120-mar 本轮跳过：暂无新的 Polymarket 行情样本".to_owned(),
            details: None,
            payload: None,
        }];

        let mut buffer = Buffer::empty(Rect::new(0, 0, 120, 8));
        render_signals(&state).render(buffer.area, &mut buffer);

        let text = buffer_text(&buffer);
        let compact = compact_text(&text);
        assert!(compact.contains("评估171次"));
        assert!(compact.contains("主要跳过"));
        assert!(compact.contains("暂无新的Polymarket行情样本"));
        assert!(compact.contains("[跳过]"));
        assert!(!compact.contains("[风控]"));
    }

    #[test]
    fn sorted_detail_items_prioritize_plan_and_result_economics() {
        let items = HashMap::from([
            ("结果状态".to_owned(), "completed".to_owned()),
            ("计划毛EdgeUSD".to_owned(), "14".to_owned()),
            ("计划费用USD".to_owned(), "2".to_owned()),
            ("计划净EdgeUSD".to_owned(), "12".to_owned()),
            ("计划边际USD".to_owned(), "12".to_owned()),
            ("计划ID".to_owned(), "plan-1".to_owned()),
            ("实际边际USD".to_owned(), "11".to_owned()),
            ("市场".to_owned(), "btc-test".to_owned()),
            ("Poly吃簿成本USD".to_owned(), "1.2".to_owned()),
            ("Poly账本条数".to_owned(), "1".to_owned()),
        ]);

        let ordered_keys = sorted_detail_items(&items)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let index = |needle: &str| {
            ordered_keys
                .iter()
                .position(|item| item == needle)
                .expect("key should exist")
        };

        assert!(index("市场") < index("计划ID"));
        assert!(index("计划ID") < index("计划毛EdgeUSD"));
        assert!(index("计划毛EdgeUSD") < index("计划费用USD"));
        assert!(index("计划费用USD") < index("计划净EdgeUSD"));
        assert!(index("计划净EdgeUSD") < index("计划边际USD"));
        assert!(index("计划净EdgeUSD") < index("Poly吃簿成本USD"));
        assert!(index("计划边际USD") < index("结果状态"));
        assert!(index("结果状态") < index("Poly账本条数"));
        assert!(index("结果状态") < index("实际边际USD"));
    }

    #[test]
    fn render_performance_shows_latest_closed_trade_summary() {
        let mut state = TuiState::default();
        state.monitor.recent_trades = vec![TradeView {
            market: "btc-dip-80-mar".to_owned(),
            opened_at_ms: 1_774_362_400_000,
            closed_at_ms: 1_774_362_516_166,
            direction: "多".to_owned(),
            token_side: "No".to_owned(),
            entry_price: Some(0.33165),
            exit_price: Some(0.29322018175),
            realized_pnl_usd: -23.649118923076923,
            correlation_id: Some("corr-1".to_owned()),
        }];

        let mut buffer = Buffer::empty(Rect::new(0, 0, 120, 9));
        render_performance(&state).render(buffer.area, &mut buffer);

        let text = buffer_text(&buffer);
        let compact = compact_text(&text);
        assert!(compact.contains("最近平仓"));
        assert!(compact.contains("btc-dip-80-mar"));
        assert!(compact.contains("-$23.65"));
    }

    #[test]
    fn render_performance_shows_strategy_health_summary_in_chinese() {
        let mut state = TuiState::default();
        state.monitor.runtime.strategy_health = polyalpha_core::StrategyHealthView {
            total_markets: 256,
            fair_value_ready_markets: 194,
            z_score_ready_markets: 0,
            warmup_ready_markets: 12,
            insufficient_warmup_markets: 182,
            stale_markets: 159,
            no_poly_markets: 62,
            cex_history_failed_markets: 3,
            poly_history_failed_markets: 7,
            min_warmup_samples: 600,
            warmup_total_markets: 256,
            warmup_completed_markets: 128,
            warmup_failed_markets: 10,
        };

        let mut buffer = Buffer::empty(Rect::new(0, 0, 180, 12));
        render_performance(&state).render(buffer.area, &mut buffer);

        let compact = compact_text(&buffer_text(&buffer));
        assert!(compact.contains("健康"));
        assert!(compact.contains("Z值就绪0/256"));
        assert!(compact.contains("公允值就绪194/256"));
        assert!(compact.contains("预热完成12/256"));
        assert!(compact.contains("预热进度128/256"));
        assert!(compact.contains("预热不足182"));
        assert!(compact.contains("Stale159"));
        assert!(compact.contains("NoPoly62"));
        assert!(compact.contains("历史失败CEX3/Poly7"));
        assert!(compact.contains("门槛600"));
    }

    #[test]
    fn render_performance_shows_pulse_summary_when_present() {
        let mut state = TuiState::default();
        state.monitor.pulse = Some(PulseMonitorView {
            active_sessions: vec![PulseSessionMonitorRow {
                session_id: "pulse-session-1".to_owned(),
                asset: "btc".to_owned(),
                state: "maker_exit_working".to_owned(),
                remaining_secs: 540,
                net_edge_bps: 31.4,
            }],
            asset_health: vec![PulseAssetHealthRow {
                asset: "btc".to_owned(),
                provider_id: Some("deribit_primary".to_owned()),
                anchor_age_ms: Some(42),
                anchor_latency_delta_ms: Some(18),
                poly_quote_age_ms: Some(15),
                cex_quote_age_ms: Some(8),
                open_sessions: 1,
                net_target_delta: Some(0.41),
                actual_exchange_position: Some(0.39),
                status: Some("warning".to_owned()),
                disable_reason: Some("hedge_drift".to_owned()),
            }],
            markets: Vec::new(),
            selected_session: Some(polyalpha_core::PulseSessionDetailView {
                session_id: "pulse-session-1".to_owned(),
                asset: "btc".to_owned(),
                state: "maker_exit_working".to_owned(),
                remaining_secs: 540,
                net_edge_bps: 31.4,
                pulse_score_bps: Some(182.5),
                planned_poly_qty: Some("10000".to_owned()),
                actual_poly_filled_qty: Some("3500".to_owned()),
                actual_poly_fill_ratio: Some(0.35),
                entry_price: Some("0.35".to_owned()),
                target_exit_price: Some("0.38".to_owned()),
                timeout_exit_price: Some("0.31".to_owned()),
                entry_executable_notional_usd: Some("250".to_owned()),
                candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
                expected_open_net_pnl_usd: Some("3.85".to_owned()),
                timeout_loss_estimate_usd: Some("21.68".to_owned()),
                required_hit_rate: Some(0.768),
                reversion_pocket_ticks: Some(4.0),
                reversion_pocket_notional_usd: Some("28.57".to_owned()),
                vacuum_ratio: Some("1".to_owned()),
                session_target_delta_exposure: Some("0.41".to_owned()),
                session_allocated_hedge_qty: Some("0.39".to_owned()),
                anchor_latency_delta_ms: Some(18),
                distance_to_mid_bps: Some(8.0),
                relative_order_age_ms: Some(900),
            }),
        });

        let mut buffer = Buffer::empty(Rect::new(0, 0, 200, 14));
        render_performance(&state).render(buffer.area, &mut buffer);

        let compact = compact_text(&buffer_text(&buffer));
        assert!(compact.contains("Pulse会话1"));
        assert!(compact.contains("活跃1"));
        assert!(compact.contains("btc"));
        assert!(compact.contains("anchor42ms"));
        assert!(compact.contains("对冲偏移"));
        assert!(compact.contains("EV$4.12"));
        assert!(compact.contains("超亏$21.68"));
        assert!(compact.contains("Hit76.8%"));
        assert!(compact.contains("入0.35"));
        assert!(compact.contains("目0.38"));
    }

    #[test]
    fn render_position_detail_shows_exit_now_economics() {
        let mut state = TuiState::default();
        state.monitor.positions = vec![PositionView {
            market: "btc-test".to_owned(),
            poly_side: "LONG YES".to_owned(),
            poly_entry_price: 0.45,
            poly_current_price: 0.48,
            poly_quantity: 100.0,
            poly_pnl_usd: 3.0,
            poly_spread_pct: 0.0,
            poly_slippage_bps: 1.0,
            cex_exchange: "Binance".to_owned(),
            cex_side: "SHORT".to_owned(),
            cex_entry_price: 79_000.0,
            cex_current_price: 80_000.0,
            cex_quantity: 0.01,
            cex_pnl_usd: 9.0,
            cex_spread_pct: 0.0,
            cex_slippage_bps: 2.0,
            basis_entry_bps: 120,
            basis_current_bps: 300,
            delta: 0.12,
            hedge_ratio: 1.0,
            total_pnl_usd: 12.0,
            realized_pnl_so_far_usd: -1.25,
            close_cost_preview_usd: Some(3.31),
            exit_now_net_pnl_usd: Some(7.44),
            holding_secs: 60,
        }];

        let mut buffer = Buffer::empty(Rect::new(0, 0, 180, 12));
        render_position_detail(&state).render(buffer.area, &mut buffer);

        let compact = compact_text(&buffer_text(&buffer));
        assert!(compact.contains("盯市总盈亏"));
        assert!(compact.contains("已实现盈亏"));
        assert!(compact.contains("立即平仓毛值"));
        assert!(compact.contains("平仓预估成本"));
        assert!(compact.contains("立即平仓净值"));
        assert!(compact.contains("+$12.00"));
        assert!(compact.contains("-$1.25"));
        assert!(compact.contains("+$10.75"));
        assert!(compact.contains("$3.31"));
        assert!(compact.contains("+$7.44"));
    }

    #[test]
    fn render_signals_shows_strategy_not_ready_root_cause() {
        let mut state = TuiState::default();
        state.monitor.runtime.strategy_health = polyalpha_core::StrategyHealthView {
            total_markets: 256,
            fair_value_ready_markets: 194,
            z_score_ready_markets: 0,
            warmup_ready_markets: 12,
            insufficient_warmup_markets: 182,
            stale_markets: 159,
            no_poly_markets: 62,
            cex_history_failed_markets: 0,
            poly_history_failed_markets: 0,
            min_warmup_samples: 600,
            warmup_total_markets: 256,
            warmup_completed_markets: 128,
            warmup_failed_markets: 0,
        };

        let mut buffer = Buffer::empty(Rect::new(0, 0, 150, 10));
        render_signals(&state).render(buffer.area, &mut buffer);

        let compact = compact_text(&buffer_text(&buffer));
        assert!(compact.contains("当前无信号主因"));
        assert!(compact.contains("Z值未形成"));
        assert!(compact.contains("预热完成12/256"));
        assert!(compact.contains("门槛600"));
    }

    #[test]
    fn market_table_marks_insufficient_warmup_in_status_column() {
        let mut market = sample_market("warmup-market", None);
        market.basis_history_len = 128;

        assert_eq!(
            format_market_status_compact(&market, 600),
            "交易池/预热128/600"
        );
    }

    #[test]
    fn market_table_marks_pricing_warmup_in_status_column() {
        let mut market = sample_market("pricing-warmup", Some(-1.0));
        market.sigma_source = Some("default".to_owned());
        market.returns_window_len = 2;

        let actual = format_market_status_compact(&market, 600);
        assert_eq!(actual, "交易池/估值预热2笔");
        assert!(display_width(&actual) <= 18, "{actual}");
    }

    #[test]
    fn command_formatters_render_chinese_labels() {
        assert_eq!(format_command_kind(&CommandKind::Pause), "暂停交易");
        assert_eq!(
            format_command_kind(&CommandKind::ClosePosition {
                market: "btc-test".to_owned()
            }),
            "平仓(btc-test)"
        );
        assert_eq!(
            format_command_kind(&CommandKind::UpdateConfig {
                config: polyalpha_core::ConfigUpdate {
                    entry_z: Some(2.5),
                    ..polyalpha_core::ConfigUpdate::default()
                }
            }),
            "调整参数(入场Z=2.50)"
        );
        assert_eq!(
            format_command_kind(&CommandKind::UpdateConfig {
                config: polyalpha_core::ConfigUpdate {
                    band_policy: Some("disabled".to_owned()),
                    ..polyalpha_core::ConfigUpdate::default()
                }
            }),
            "调整参数(价格带策略=disabled)"
        );
        assert_eq!(
            format_command_status(CommandStatus::PartialSuccess),
            "部分成功"
        );
    }

    fn sample_market(symbol: &str, z_score: Option<f64>) -> MarketView {
        MarketView {
            symbol: symbol.to_owned(),
            z_score,
            basis_pct: Some(1.23),
            poly_price: Some(0.44),
            poly_yes_price: Some(0.44),
            poly_no_price: Some(0.56),
            cex_price: Some(102.0),
            fair_value: Some(101.5),
            has_position: false,
            minutes_to_expiry: Some(15.0),
            basis_history_len: 640,
            raw_sigma: Some(0.00016),
            effective_sigma: Some(0.00018),
            sigma_source: Some("realized".to_owned()),
            returns_window_len: 64,
            active_token_side: Some("YES".to_owned()),
            poly_updated_at_ms: Some(1_000),
            cex_updated_at_ms: Some(1_500),
            poly_quote_age_ms: Some(500),
            cex_quote_age_ms: Some(100),
            cross_leg_skew_ms: Some(400),
            evaluable_status: EvaluableStatus::Evaluable,
            async_classification: AsyncClassification::PolyLagAcceptable,
            market_tier: MarketTier::Tradeable,
            retention_reason: MarketRetentionReason::None,
            last_focus_at_ms: Some(1_800),
            last_tradeable_at_ms: Some(1_900),
        }
    }

    fn sample_command(market: &str) -> CommandView {
        CommandView {
            command_id: format!("cmd-{market}"),
            kind: CommandKind::ClosePosition {
                market: market.to_owned(),
            },
            status: CommandStatus::Failed,
            created_at_ms: 1_000,
            updated_at_ms: 2_000,
            finished: true,
            timed_out: false,
            cancellable: true,
            message: Some("命令已取消".to_owned()),
            error_code: Some(1004),
        }
    }

    fn row_symbols(buffer: &Buffer, y: u16) -> Vec<String> {
        (0..buffer.area.width)
            .map(|x| {
                buffer
                    .cell((x, y))
                    .map(|cell| cell.symbol().to_owned())
                    .unwrap_or_default()
            })
            .collect()
    }

    fn buffer_text(buffer: &Buffer) -> String {
        (0..buffer.area.height)
            .map(|y| row_symbols(buffer, y).join(""))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn compact_text(text: &str) -> String {
        text.chars().filter(|ch| !ch.is_whitespace()).collect()
    }

    fn find_sequence(row: &[String], sequence: &[&str]) -> Option<usize> {
        row.windows(sequence.len()).position(|window| {
            window
                .iter()
                .zip(sequence)
                .all(|(cell, expected)| cell == expected)
        })
    }
}
