mod args;
mod backtest;
mod commands;
mod markets;
mod sim;

use anyhow::Result;
use clap::Parser;

use args::{Cli, Command, MarketCommand, SimCommand};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
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
        Command::Backtest { command } => backtest::run_backtest_command(command)?,
    }

    Ok(())
}
