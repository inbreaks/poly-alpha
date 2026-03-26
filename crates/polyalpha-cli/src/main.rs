mod args;
mod audit;
mod backtest;
mod backtest_rust;
mod commands;
mod markets;
mod monitor;
mod paper;
mod sim;

use anyhow::Result;
use clap::Parser;

use args::{Cli, Command, MarketCommand, PaperCommand, SimCommand};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Audit { command } => audit::run_audit_command(command)?,
        Command::CheckConfig { env } => commands::run_check_config(&env)?,
        Command::Demo { env } => commands::run_demo(&env).await?,
        Command::LiveDataCheck {
            env,
            market_index,
            depth,
        } => commands::run_live_data_check(&env, market_index, depth).await?,
        Command::LiveExecPreview {
            env,
            market_index,
            exchange,
            side,
            order_type,
            qty,
            price,
            reduce_only,
        } => {
            commands::run_live_exec_preview(
                &env,
                market_index,
                exchange,
                side,
                order_type,
                &qty,
                price.as_deref(),
                reduce_only,
            )
            .await?
        }
        Command::Markets { command } => match command {
            MarketCommand::DiscoverBtc {
                env,
                template_market_index,
                query,
                match_text,
                limit,
                pick,
                output,
            } => {
                markets::run_discover_btc(
                    &env,
                    template_market_index,
                    &query,
                    match_text.as_deref(),
                    limit,
                    pick,
                    output.as_deref(),
                )
                .await?
            }
            MarketCommand::DiscoverEth {
                env,
                template_market_index,
                query,
                match_text,
                limit,
                pick,
                output,
            } => {
                markets::run_discover_eth(
                    &env,
                    template_market_index,
                    &query,
                    match_text.as_deref(),
                    limit,
                    pick,
                    output.as_deref(),
                )
                .await?
            }
            MarketCommand::DiscoverSol {
                env,
                template_market_index,
                query,
                match_text,
                limit,
                pick,
                output,
            } => {
                markets::run_discover_sol(
                    &env,
                    template_market_index,
                    &query,
                    match_text.as_deref(),
                    limit,
                    pick,
                    output.as_deref(),
                )
                .await?
            }
            MarketCommand::DiscoverXrp {
                env,
                template_market_index,
                query,
                match_text,
                limit,
                pick,
                output,
            } => {
                markets::run_discover_xrp(
                    &env,
                    template_market_index,
                    &query,
                    match_text.as_deref(),
                    limit,
                    pick,
                    output.as_deref(),
                )
                .await?
            }
            MarketCommand::RefreshActive {
                base_env,
                catalog_env,
                asset,
                limit_per_asset,
                max_markets_per_asset,
                min_minutes_to_settlement,
                min_liquidity_usd,
                min_volume_24h_usd,
                output,
                report_json,
            } => {
                markets::run_refresh_active(
                    &base_env,
                    &catalog_env,
                    &asset,
                    limit_per_asset,
                    max_markets_per_asset,
                    min_minutes_to_settlement,
                    min_liquidity_usd,
                    min_volume_24h_usd,
                    output.as_deref(),
                    report_json.as_deref(),
                )
                .await?
            }
        },
        Command::Sim { command } => match command {
            SimCommand::Run {
                env,
                market_index,
                scenario,
                tick_interval_ms,
                print_every,
                max_ticks,
                json,
            } => {
                sim::run_sim(
                    &env,
                    market_index,
                    scenario,
                    tick_interval_ms,
                    print_every,
                    max_ticks,
                    json,
                )
                .await?
            }
            SimCommand::Inspect { env, format } => sim::inspect_sim(&env, format)?,
        },
        Command::Paper { command } => match command {
            PaperCommand::Run {
                env,
                market_index,
                poll_interval_ms,
                print_every,
                max_ticks,
                depth,
                include_funding,
                json,
                warmup_klines,
            } => {
                paper::run_paper(
                    &env,
                    market_index,
                    poll_interval_ms,
                    print_every,
                    max_ticks,
                    depth,
                    include_funding,
                    json,
                    warmup_klines,
                )
                .await?
            }
            PaperCommand::RunMulti {
                env,
                poll_interval_ms,
                print_every,
                max_ticks,
                depth,
                include_funding,
                json,
                warmup_klines,
            } => {
                paper::run_paper_multi(
                    &env,
                    poll_interval_ms,
                    print_every,
                    max_ticks,
                    depth,
                    include_funding,
                    json,
                    warmup_klines,
                )
                .await?
            }
            PaperCommand::Inspect { env, format } => paper::inspect_paper(&env, format)?,
        },
        Command::Backtest { command } => backtest::run_backtest_command(command).await?,
        Command::Monitor {
            socket,
            json,
            snapshot,
            interval_ms,
            timeout_ms,
            market,
            mouse_capture,
            no_alternate_screen,
        } => match monitor::resolve_monitor_mode(json, snapshot)? {
            monitor::MonitorMode::Snapshot => {
                monitor::run_snapshot(&socket, timeout_ms, market.as_deref()).await?
            }
            monitor::MonitorMode::Json => {
                monitor::run_json_mode(&socket, interval_ms, market.as_deref()).await?
            }
            monitor::MonitorMode::Tui => {
                monitor::run_monitor(
                    &socket,
                    mouse_capture,
                    !no_alternate_screen,
                    market.as_deref(),
                )
                .await?
            }
        },
        Command::MonitorJson {
            socket,
            interval_ms,
            market,
        } => monitor::run_json_mode(&socket, interval_ms, market.as_deref()).await?,
        Command::MonitorSnapshot {
            socket,
            timeout_ms,
            market,
        } => monitor::run_snapshot(&socket, timeout_ms, market.as_deref()).await?,
    }

    Ok(())
}
