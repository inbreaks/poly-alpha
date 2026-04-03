//! TUI 应用主循环

use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError as AckTryRecvError;

use polyalpha_core::{CommandKind, ConfigUpdate, Message, MonitorSocketClient, MonitorState};

use super::filter_state_for_market;
use super::state::{BannerLevel, ConfigField, Dialog, LogPane, Page, TuiState};
use super::ui::{
    format_command_kind, format_command_status, render_command_detail, render_connections,
    render_dialog_overlay, render_equity_chart, render_equity_toolbar, render_event_details,
    render_event_log, render_header, render_help_bar, render_help_overlay, render_log_commands,
    render_log_filters, render_markets, render_performance, render_position_detail,
    render_position_list, render_positions_summary, render_recent_trades, render_signals,
    render_trade_detail,
};

/// 键盘轮询间隔
const INPUT_POLL_INTERVAL_MS: u64 = 50;
/// 空闲重绘间隔，用于刷新年龄/运行时长等本地时间派生字段
const IDLE_REDRAW_INTERVAL_MS: u64 = 1_000;

struct PendingAck {
    label: String,
    receiver: oneshot::Receiver<Message>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MonitorLoopTiming {
    redraw_due: bool,
    poll_timeout: Duration,
}

fn monitor_loop_timing(
    now: Instant,
    last_draw_at: Instant,
    state_dirty: bool,
) -> MonitorLoopTiming {
    if state_dirty {
        return MonitorLoopTiming {
            redraw_due: true,
            poll_timeout: Duration::ZERO,
        };
    }

    let idle_redraw_interval = Duration::from_millis(IDLE_REDRAW_INTERVAL_MS);
    let since_draw = now.saturating_duration_since(last_draw_at);
    if since_draw >= idle_redraw_interval {
        return MonitorLoopTiming {
            redraw_due: true,
            poll_timeout: Duration::ZERO,
        };
    }

    MonitorLoopTiming {
        redraw_due: false,
        poll_timeout: idle_redraw_interval
            .saturating_sub(since_draw)
            .min(Duration::from_millis(INPUT_POLL_INTERVAL_MS)),
    }
}

#[derive(Clone, Copy, Debug)]
struct TerminalMode {
    mouse_capture: bool,
    alternate_screen: bool,
}

#[derive(Debug, Default)]
struct TerminalCleanup {
    raw_mode: bool,
    mouse_capture: bool,
    alternate_screen: bool,
}

impl TerminalCleanup {
    fn enter(mode: TerminalMode) -> Result<Self> {
        let mut cleanup = Self::default();

        crossterm::terminal::enable_raw_mode()?;
        cleanup.raw_mode = true;

        let mut stdout = std::io::stdout();
        if mode.alternate_screen {
            crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
            cleanup.alternate_screen = true;
        }
        if mode.mouse_capture {
            crossterm::execute!(stdout, crossterm::event::EnableMouseCapture)?;
            cleanup.mouse_capture = true;
        }

        Ok(cleanup)
    }

    fn restore(&mut self) -> Result<()> {
        let mut first_error: Option<anyhow::Error> = None;

        if self.raw_mode {
            if let Err(err) = crossterm::terminal::disable_raw_mode() {
                if first_error.is_none() {
                    first_error = Some(err.into());
                }
            }
            self.raw_mode = false;
        }

        let mut stdout = std::io::stdout();
        if self.mouse_capture {
            if let Err(err) = crossterm::execute!(stdout, crossterm::event::DisableMouseCapture) {
                if first_error.is_none() {
                    first_error = Some(err.into());
                }
            }
            self.mouse_capture = false;
        }

        if self.alternate_screen {
            if let Err(err) = crossterm::execute!(stdout, crossterm::terminal::LeaveAlternateScreen)
            {
                if first_error.is_none() {
                    first_error = Some(err.into());
                }
            }
            self.alternate_screen = false;
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl Drop for TerminalCleanup {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

/// 运行 TUI 监控
pub async fn run_monitor(
    socket_path: &str,
    mouse_capture: bool,
    alternate_screen: bool,
    market_filter: Option<&str>,
) -> Result<()> {
    let mut cleanup = TerminalCleanup::enter(TerminalMode {
        mouse_capture,
        alternate_screen,
    })?;

    let backend = CrosstermBackend::new(std::io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut client = MonitorSocketClient::new(socket_path);
    client.connect().await?;

    let mut state_rx = client
        .take_state_receiver()
        .ok_or_else(|| anyhow::anyhow!("获取状态接收器失败"))?;

    let mut tui_state = TuiState::default();
    let result = run_main_loop(
        &mut terminal,
        &mut tui_state,
        &mut state_rx,
        &client,
        market_filter,
    )
    .await;
    client.disconnect();

    drop(terminal);
    if let Err(cleanup_err) = cleanup.restore() {
        return match result {
            Ok(()) => Err(cleanup_err),
            Err(run_err) => Err(run_err.context(format!("终端状态恢复失败: {cleanup_err}"))),
        };
    }

    result
}

async fn run_main_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    tui_state: &mut TuiState,
    state_rx: &mut tokio::sync::mpsc::Receiver<MonitorState>,
    client: &MonitorSocketClient,
    market_filter: Option<&str>,
) -> Result<()> {
    let mut pending_acks: Vec<PendingAck> = Vec::new();
    let mut state_dirty = true;
    let mut last_draw_at = Instant::now();

    loop {
        let mut received_state = false;
        while let Ok(state) = state_rx.try_recv() {
            tui_state.update(filter_state_for_market(state, market_filter));
            received_state = true;
        }
        state_dirty |= received_state;

        state_dirty |= poll_pending_acks(tui_state, &mut pending_acks);

        let timing = monitor_loop_timing(Instant::now(), last_draw_at, state_dirty);
        if timing.redraw_due {
            terminal.draw(|frame| {
                let size = frame.area();
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Length(7),
                        Constraint::Length(6),
                        Constraint::Min(10),
                        Constraint::Length(3),
                        Constraint::Length(1),
                    ])
                    .split(size);

                frame.render_widget(render_header(tui_state), chunks[0]);
                frame.render_widget(render_performance(tui_state), chunks[1]);
                frame.render_widget(render_signals(tui_state), chunks[2]);

                match tui_state.page {
                    Page::Overview => {
                        let main = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
                            .split(chunks[3]);
                        frame.render_widget(render_positions_summary(tui_state), main[0]);
                        frame.render_widget(render_markets(tui_state), main[1]);
                    }
                    Page::Positions => {
                        let main = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
                            .split(chunks[3]);
                        frame.render_widget(render_position_list(tui_state), main[0]);
                        frame.render_widget(render_position_detail(tui_state), main[1]);
                    }
                    Page::Equity => {
                        let main = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Length(5), Constraint::Min(10)])
                            .split(chunks[3]);
                        frame.render_widget(render_equity_toolbar(tui_state), main[0]);
                        if tui_state.show_trade_detail() {
                            let body = Layout::default()
                                .direction(Direction::Vertical)
                                .constraints([
                                    Constraint::Percentage(46),
                                    Constraint::Percentage(54),
                                ])
                                .split(main[1]);
                            let upper = Layout::default()
                                .direction(Direction::Horizontal)
                                .constraints([
                                    Constraint::Percentage(64),
                                    Constraint::Percentage(36),
                                ])
                                .split(body[0]);
                            frame.render_widget(render_equity_chart(tui_state), upper[0]);
                            frame.render_widget(render_trade_detail(tui_state), upper[1]);
                            frame.render_widget(render_recent_trades(tui_state), body[1]);
                        } else {
                            let body = Layout::default()
                                .direction(Direction::Vertical)
                                .constraints([
                                    Constraint::Percentage(40),
                                    Constraint::Percentage(60),
                                ])
                                .split(main[1]);
                            frame.render_widget(render_equity_chart(tui_state), body[0]);
                            frame.render_widget(render_recent_trades(tui_state), body[1]);
                        }
                    }
                    Page::Log => {
                        let main = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Length(5), Constraint::Min(8)])
                            .split(chunks[3]);
                        frame.render_widget(render_log_filters(tui_state), main[0]);
                        let body = Layout::default()
                            .direction(Direction::Horizontal)
                            .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
                            .split(main[1]);
                        let lists = Layout::default()
                            .direction(Direction::Vertical)
                            .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
                            .split(body[0]);
                        frame.render_widget(render_event_log(tui_state), lists[0]);
                        frame.render_widget(render_log_commands(tui_state), lists[1]);
                        match tui_state.active_log_pane {
                            LogPane::Events => {
                                frame.render_widget(render_event_details(tui_state), body[1]);
                            }
                            LogPane::Commands => {
                                frame.render_widget(render_command_detail(tui_state), body[1]);
                            }
                        }
                    }
                }

                frame.render_widget(render_connections(tui_state), chunks[4]);
                frame.render_widget(render_help_bar(tui_state), chunks[5]);
                render_help_overlay(frame, tui_state);
                render_dialog_overlay(frame, tui_state);
            })?;
            last_draw_at = Instant::now();
            state_dirty = false;
        }

        if event::poll(timing.poll_timeout)? {
            match event::read()? {
                Event::Key(key) => {
                    state_dirty = true;
                    if tui_state.has_dialog() {
                        match key.code {
                            KeyCode::Left | KeyCode::Char('h') => tui_state.nudge_dialog(-1),
                            KeyCode::Right | KeyCode::Char('l') => tui_state.nudge_dialog(1),
                            KeyCode::Enter => {
                                handle_dialog_confirm(tui_state, &mut pending_acks, client).await;
                            }
                            KeyCode::Esc => tui_state.close_dialog(),
                            _ => {}
                        }
                        continue;
                    }

                    if tui_state.show_help {
                        match key.code {
                            KeyCode::Char('q') => break,
                            KeyCode::Esc | KeyCode::Char('?') => tui_state.toggle_help(),
                            _ => {}
                        }
                        continue;
                    }

                    if tui_state.is_editing_log_search() {
                        handle_log_search_input(tui_state, key.code);
                        continue;
                    }

                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('1') => tui_state.switch_page(Page::Overview),
                        KeyCode::Char('2') => tui_state.switch_page(Page::Positions),
                        KeyCode::Char('3') => tui_state.switch_page(Page::Equity),
                        KeyCode::Char('4') => {
                            tui_state.switch_page(Page::Log);
                            tui_state.mark_active_log_pane_read();
                        }
                        KeyCode::Char('?') => tui_state.toggle_help(),
                        KeyCode::Enter => {
                            if tui_state.page == Page::Equity {
                                tui_state.toggle_trade_detail();
                                tui_state.set_banner(
                                    if tui_state.show_trade_detail() {
                                        "交易详情已展开"
                                    } else {
                                        "交易详情已收起"
                                    },
                                    BannerLevel::Info,
                                );
                            }
                        }
                        KeyCode::Char('z') | KeyCode::Char('Z') => {
                            tui_state.open_adjust_config(ConfigField::EntryZ);
                        }
                        KeyCode::Char('X') => {
                            tui_state.open_adjust_config(ConfigField::ExitZ);
                        }
                        KeyCode::Char('s') | KeyCode::Char('S') => {
                            tui_state.open_adjust_config(ConfigField::PositionNotionalUsd);
                        }
                        KeyCode::Char('w') | KeyCode::Char('W') => {
                            tui_state.open_adjust_config(ConfigField::RollingWindowSecs);
                        }
                        KeyCode::Char('d') | KeyCode::Char('D') => {
                            export_current_view(tui_state);
                        }
                        KeyCode::Tab => {
                            if tui_state.page == Page::Log {
                                tui_state.toggle_log_pane();
                                tui_state.mark_active_log_pane_read();
                            }
                        }
                        KeyCode::Char('/') => {
                            if tui_state.page == Page::Log {
                                tui_state.begin_log_search();
                                let target = tui_state
                                    .log_search_target()
                                    .map(|target| target.label())
                                    .unwrap_or("日志");
                                tui_state.set_banner(
                                    format!("{target}搜索：输入关键字，Enter 完成，Esc 退出"),
                                    BannerLevel::Info,
                                );
                            }
                        }
                        KeyCode::Char('[') => {
                            if tui_state.page == Page::Log {
                                tui_state.cycle_active_log_filter(-1);
                                announce_log_filter_change(tui_state);
                            }
                        }
                        KeyCode::Char(']') => {
                            if tui_state.page == Page::Log {
                                tui_state.cycle_active_log_filter(1);
                                announce_log_filter_change(tui_state);
                            }
                        }
                        KeyCode::Char('t') | KeyCode::Char('T') => {
                            if tui_state.page == Page::Equity {
                                tui_state.cycle_equity_range(1);
                                tui_state.set_banner(
                                    format!("净值时间范围：{}", tui_state.equity_range().label()),
                                    BannerLevel::Info,
                                );
                            } else if tui_state.page == Page::Log {
                                tui_state.cycle_active_log_filter(1);
                                announce_log_filter_change(tui_state);
                            }
                        }
                        KeyCode::Char('r') | KeyCode::Char('R') => {
                            if tui_state.page == Page::Log {
                                let marked = tui_state.mark_active_log_pane_read();
                                let target = active_log_target_label(tui_state);
                                if marked == 0 {
                                    tui_state.set_banner(
                                        format!("{target}当前筛选结果没有新的未读项"),
                                        BannerLevel::Info,
                                    );
                                } else {
                                    tui_state.set_banner(
                                        format!("{target}已标记 {} 条为已读", marked),
                                        BannerLevel::Success,
                                    );
                                }
                            }
                        }
                        KeyCode::Char('p') | KeyCode::Char('P') => {
                            let command = if tui_state.monitor.paused {
                                CommandKind::Resume
                            } else {
                                CommandKind::Pause
                            };
                            queue_command(tui_state, &mut pending_acks, client, command).await;
                        }
                        KeyCode::Char('e') | KeyCode::Char('E') => {
                            let dialog = if tui_state.monitor.emergency {
                                Dialog::ClearEmergencyConfirm
                            } else {
                                Dialog::EmergencyStopConfirm
                            };
                            tui_state.show_dialog(dialog);
                        }
                        KeyCode::Char('c') | KeyCode::Char('C') => {
                            if tui_state.page == Page::Log {
                                let cleared = tui_state.clear_active_log_pane_read();
                                let target = active_log_target_label(tui_state);
                                if cleared == 0 {
                                    tui_state.set_banner(
                                        format!("{target}当前没有可清理的已读项"),
                                        BannerLevel::Info,
                                    );
                                } else {
                                    tui_state.set_banner(
                                        format!("{target}已清理 {} 条已读记录", cleared),
                                        BannerLevel::Success,
                                    );
                                }
                            } else {
                                let market_to_close =
                                    match tui_state.page {
                                        Page::Overview => tui_state
                                            .selected_market()
                                            .and_then(|market| {
                                                tui_state.monitor.positions.iter().find(
                                                    |position| position.market == market.symbol,
                                                )
                                            })
                                            .map(|position| position.market.clone()),
                                        _ => tui_state
                                            .selected_position()
                                            .map(|position| position.market.clone()),
                                    };

                                if let Some(market) = market_to_close {
                                    tui_state.show_dialog(Dialog::ClosePositionConfirm { market });
                                } else if tui_state.page == Page::Overview {
                                    tui_state.set_banner(
                                        "当前选中的市场没有持仓可平",
                                        BannerLevel::Warning,
                                    );
                                } else {
                                    tui_state
                                        .set_banner("当前没有可平仓持仓", BannerLevel::Warning);
                                }
                            }
                        }
                        KeyCode::Char('a') | KeyCode::Char('A') => {
                            if tui_state.monitor.positions.is_empty() {
                                tui_state.set_banner("当前没有持仓", BannerLevel::Warning);
                            } else {
                                tui_state.show_dialog(Dialog::CloseAllPositionsConfirm);
                            }
                        }
                        KeyCode::Char('x') => {
                            if tui_state.page == Page::Log
                                && tui_state.active_log_pane == LogPane::Commands
                            {
                                if let Some(command) = tui_state.selected_command() {
                                    if command.cancellable {
                                        tui_state.show_dialog(Dialog::CancelCommandConfirm {
                                            command_id: command.command_id.clone(),
                                            summary: format_command_label(&command.kind),
                                        });
                                    } else {
                                        tui_state.set_banner(
                                            "选中的命令当前不可取消",
                                            BannerLevel::Warning,
                                        );
                                    }
                                }
                            }
                        }
                        KeyCode::Left | KeyCode::Char('h') => {
                            if tui_state.page == Page::Equity {
                                tui_state.cycle_equity_range(-1);
                                tui_state.set_banner(
                                    format!("净值时间范围：{}", tui_state.equity_range().label()),
                                    BannerLevel::Info,
                                );
                            }
                        }
                        KeyCode::Right | KeyCode::Char('l') => {
                            if tui_state.page == Page::Equity {
                                tui_state.cycle_equity_range(1);
                                tui_state.set_banner(
                                    format!("净值时间范围：{}", tui_state.equity_range().label()),
                                    BannerLevel::Info,
                                );
                            }
                        }
                        KeyCode::Down | KeyCode::Char('j') => match tui_state.page {
                            Page::Overview => tui_state.select_next_market(),
                            Page::Positions => tui_state.select_next_position(),
                            Page::Equity => tui_state.select_next_trade(),
                            Page::Log => match tui_state.active_log_pane {
                                LogPane::Events => {
                                    tui_state.select_next_event();
                                    tui_state.mark_active_log_pane_read();
                                }
                                LogPane::Commands => {
                                    tui_state.select_next_command();
                                    tui_state.mark_active_log_pane_read();
                                }
                            },
                        },
                        KeyCode::Up | KeyCode::Char('k') => match tui_state.page {
                            Page::Overview => tui_state.select_prev_market(),
                            Page::Positions => tui_state.select_prev_position(),
                            Page::Equity => tui_state.select_prev_trade(),
                            Page::Log => match tui_state.active_log_pane {
                                LogPane::Events => {
                                    tui_state.select_prev_event();
                                    tui_state.mark_active_log_pane_read();
                                }
                                LogPane::Commands => {
                                    tui_state.select_prev_command();
                                    tui_state.mark_active_log_pane_read();
                                }
                            },
                        },
                        _ => {}
                    }
                }
                Event::Resize(_, _) => {
                    state_dirty = true;
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn handle_log_search_input(tui_state: &mut TuiState, key_code: KeyCode) {
    match key_code {
        KeyCode::Esc => tui_state.cancel_log_search(),
        KeyCode::Enter => tui_state.finish_log_search(),
        KeyCode::Backspace => tui_state.pop_log_search_char(),
        KeyCode::Delete => tui_state.clear_active_log_search(),
        KeyCode::Char(ch) => tui_state.push_log_search_char(ch),
        _ => {}
    }
}

fn active_log_target_label(tui_state: &TuiState) -> &'static str {
    match tui_state.active_log_pane {
        LogPane::Events => "事件流",
        LogPane::Commands => "命令流",
    }
}

fn announce_log_filter_change(tui_state: &mut TuiState) {
    let (target, filter_label) = match tui_state.active_log_pane {
        LogPane::Events => ("事件类型", tui_state.event_filter().label()),
        LogPane::Commands => ("命令状态", tui_state.command_filter().label()),
    };
    tui_state.set_banner(format!("{target}筛选：{filter_label}"), BannerLevel::Info);
}

fn poll_pending_acks(tui_state: &mut TuiState, pending_acks: &mut Vec<PendingAck>) -> bool {
    let mut state_changed = false;
    let previous_pending_count = tui_state.pending_ack_count;
    let mut remaining = Vec::new();
    for mut pending in pending_acks.drain(..) {
        match pending.receiver.try_recv() {
            Ok(message) => {
                handle_command_ack(tui_state, &pending.label, message);
                state_changed = true;
            }
            Err(AckTryRecvError::Empty) => remaining.push(pending),
            Err(AckTryRecvError::Closed) => {
                tui_state.set_banner("命令 ACK 通道已关闭", BannerLevel::Error);
                state_changed = true;
            }
        }
    }
    *pending_acks = remaining;
    tui_state.set_pending_ack_count(pending_acks.len());
    state_changed || previous_pending_count != pending_acks.len()
}

fn handle_command_ack(tui_state: &mut TuiState, label: &str, message: Message) {
    if let Message::CommandAck {
        status,
        success,
        message,
        error_code,
        finished,
        timed_out,
        cancellable,
        ..
    } = message
    {
        let mut text = format!("{label}: {}", format_command_status(status));
        if let Some(message) = message {
            text.push_str(" · ");
            text.push_str(&message);
        }
        if let Some(code) = error_code {
            text.push_str(&format!("（错误码={code}）"));
        }
        if timed_out {
            text.push_str(" · 已超时");
        }
        if !finished {
            text.push_str(" · 等待状态同步");
        } else if cancellable {
            text.push_str(" · 仍可取消");
        }

        let level = if success {
            if finished {
                BannerLevel::Success
            } else {
                BannerLevel::Info
            }
        } else {
            BannerLevel::Error
        };
        tui_state.set_banner(text, level);
    }
}

async fn queue_command(
    tui_state: &mut TuiState,
    pending_acks: &mut Vec<PendingAck>,
    client: &MonitorSocketClient,
    command: CommandKind,
) {
    let label = format_command_label(&command);
    match client.send_command(command).await {
        Ok(receiver) => {
            pending_acks.push(PendingAck {
                label: label.clone(),
                receiver,
            });
            tui_state.set_pending_ack_count(pending_acks.len());
            tui_state.set_banner(format!("{label}: 已发送"), BannerLevel::Info);
        }
        Err(err) => {
            tui_state.set_banner(format!("{label}: 发送失败：{err}"), BannerLevel::Error);
        }
    }
}

async fn handle_dialog_confirm(
    tui_state: &mut TuiState,
    pending_acks: &mut Vec<PendingAck>,
    client: &MonitorSocketClient,
) {
    let command = match tui_state.dialog.clone() {
        Some(Dialog::ClosePositionConfirm { market }) => {
            Some(CommandKind::ClosePosition { market })
        }
        Some(Dialog::CloseAllPositionsConfirm) => Some(CommandKind::CloseAllPositions),
        Some(Dialog::EmergencyStopConfirm) => Some(CommandKind::EmergencyStop),
        Some(Dialog::ClearEmergencyConfirm) => Some(CommandKind::ClearEmergency),
        Some(Dialog::CancelCommandConfirm { command_id, .. }) => {
            Some(CommandKind::CancelCommand { command_id })
        }
        Some(Dialog::AdjustConfig { field, draft, .. }) => Some(CommandKind::UpdateConfig {
            config: config_update_from_dialog(field, draft),
        }),
        None => None,
    };

    tui_state.close_dialog();
    if let Some(command) = command {
        queue_command(tui_state, pending_acks, client, command).await;
    }
}

fn format_command_label(command: &CommandKind) -> String {
    format_command_kind(command)
}

fn config_update_from_dialog(field: ConfigField, draft: f64) -> ConfigUpdate {
    let mut config = ConfigUpdate::default();
    match field {
        ConfigField::EntryZ => config.entry_z = Some(draft),
        ConfigField::ExitZ => config.exit_z = Some(draft),
        ConfigField::PositionNotionalUsd => config.position_notional_usd = Some(draft),
        ConfigField::RollingWindowSecs => config.rolling_window = Some(draft.round() as u64),
    }
    config
}

fn export_current_view(tui_state: &mut TuiState) {
    match try_export_current_view(tui_state) {
        Ok(message) => tui_state.set_banner(message, BannerLevel::Success),
        Err(err) => tui_state.set_banner(format!("导出失败: {err}"), BannerLevel::Error),
    }
}

fn try_export_current_view(tui_state: &TuiState) -> Result<String> {
    let export_root = std::env::current_dir()?.join("monitor-exports");
    fs::create_dir_all(&export_root)?;

    let timestamp_ms = tui_state.monitor.timestamp_ms.max(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    );

    let snapshot_path = export_root.join(format!("monitor-state-{timestamp_ms}.json"));
    fs::write(
        &snapshot_path,
        serde_json::to_vec_pretty(&tui_state.monitor)?,
    )?;

    let mut exported = vec![snapshot_path.display().to_string()];
    match tui_state.page {
        Page::Equity => {
            let trades_path = export_root.join(format!("monitor-trades-{timestamp_ms}.jsonl"));
            write_jsonl(&trades_path, &tui_state.monitor.recent_trades)?;
            exported.push(trades_path.display().to_string());
        }
        Page::Log => {
            let events_path = export_root.join(format!("monitor-events-{timestamp_ms}.jsonl"));
            write_jsonl(&events_path, &tui_state.monitor.recent_events)?;
            exported.push(events_path.display().to_string());

            let commands_path = export_root.join(format!("monitor-commands-{timestamp_ms}.jsonl"));
            write_jsonl(&commands_path, &tui_state.monitor.recent_commands)?;
            exported.push(commands_path.display().to_string());
        }
        _ => {}
    }

    Ok(format!("已导出 {}", exported.join(", ")))
}

fn write_jsonl<T: serde::Serialize>(path: &std::path::Path, items: &[T]) -> Result<()> {
    let mut body = String::new();
    for item in items {
        body.push_str(&serde_json::to_string(item)?);
        body.push('\n');
    }
    fs::write(path, body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        monitor_loop_timing, MonitorLoopTiming, IDLE_REDRAW_INTERVAL_MS, INPUT_POLL_INTERVAL_MS,
    };

    #[test]
    fn dirty_state_redraws_immediately() {
        let now = Instant::now();
        let timing = monitor_loop_timing(now, now, true);

        assert_eq!(
            timing,
            MonitorLoopTiming {
                redraw_due: true,
                poll_timeout: Duration::ZERO,
            }
        );
    }

    #[test]
    fn idle_loop_waits_before_redrawing_again() {
        let now = Instant::now();
        let last_draw_at = now - Duration::from_millis(200);

        let timing = monitor_loop_timing(now, last_draw_at, false);

        assert_eq!(timing.redraw_due, false);
        assert_eq!(
            timing.poll_timeout,
            Duration::from_millis(INPUT_POLL_INTERVAL_MS)
        );
    }

    #[test]
    fn idle_loop_forces_redraw_after_refresh_interval() {
        let now = Instant::now();
        let last_draw_at = now - Duration::from_millis(IDLE_REDRAW_INTERVAL_MS + 1);

        let timing = monitor_loop_timing(now, last_draw_at, false);

        assert_eq!(
            timing,
            MonitorLoopTiming {
                redraw_due: true,
                poll_timeout: Duration::ZERO,
            }
        );
    }
}
