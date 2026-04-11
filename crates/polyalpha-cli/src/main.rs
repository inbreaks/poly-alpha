mod args;
mod audit;
mod backtest;
mod backtest_rust;
mod commands;
mod market_pool;
mod markets;
mod monitor;
mod paper;
mod pulse;
mod runtime;
mod sim;
mod strategy_scope;

use anyhow::Result;
use clap::Parser;

use args::{Cli, Command, LiveCommand, MarketCommand, PaperCommand, PulseCommand, SimCommand};

fn install_rustls_crypto_provider() -> Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }

    match rustls::crypto::ring::default_provider().install_default() {
        Ok(()) => Ok(()),
        Err(_) if rustls::crypto::CryptoProvider::get_default().is_some() => Ok(()),
        Err(_) => Err(anyhow::anyhow!("failed to install rustls crypto provider")),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    install_rustls_crypto_provider()?;
    let cli = Cli::parse();

    match cli.command {
        Command::Audit { command } => audit::run_audit_command(command)?,
        Command::CheckConfig { env } => commands::run_check_config(&env)?,
        Command::Demo { env } => commands::run_demo(&env).await?,
        Command::Live { command } => match command {
            LiveCommand::Run {
                env,
                market_index,
                poll_interval_ms,
                print_every,
                max_ticks,
                depth,
                include_funding,
                json,
                warmup_klines,
                executor_mode,
                confirm_live,
            } => {
                paper::run_live(
                    &env,
                    market_index,
                    poll_interval_ms,
                    print_every,
                    max_ticks,
                    depth,
                    include_funding,
                    json,
                    warmup_klines,
                    executor_mode,
                    confirm_live,
                )
                .await?
            }
            LiveCommand::RunMulti {
                env,
                poll_interval_ms,
                print_every,
                max_ticks,
                depth,
                include_funding,
                json,
                warmup_klines,
                executor_mode,
                confirm_live,
            } => {
                paper::run_live_multi(
                    &env,
                    poll_interval_ms,
                    print_every,
                    max_ticks,
                    depth,
                    include_funding,
                    json,
                    warmup_klines,
                    executor_mode,
                    confirm_live,
                )
                .await?
            }
            LiveCommand::DataCheck {
                env,
                market_index,
                depth,
            } => commands::run_live_data_check(&env, market_index, depth).await?,
            LiveCommand::ExecPreview {
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
            LiveCommand::Inspect { env, format } => paper::inspect_live(&env, format)?,
            LiveCommand::Pulse { command } => match command {
                PulseCommand::Run {
                    env,
                    assets,
                    executor_mode,
                } => {
                    let parsed_assets = pulse::parse_pulse_assets(&assets)?;
                    pulse::run_pulse_live_mock(&env, &parsed_assets, executor_mode).await?
                }
            },
        },
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
            PaperCommand::Pulse { command } => match command {
                PulseCommand::Run { env, assets, .. } => {
                    let parsed_assets = pulse::parse_pulse_assets(&assets)?;
                    pulse::run_pulse_paper(&env, &parsed_assets).await?
                }
            },
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

#[cfg(test)]
mod tests {
    #[test]
    fn install_rustls_crypto_provider_sets_process_default() {
        super::install_rustls_crypto_provider().expect("install rustls crypto provider");
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
