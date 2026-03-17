use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use tokio::sync::broadcast::error::TryRecvError;

use polyalpha_core::{
    AlphaEngine, ArbSignalAction, ArbSignalEvent, CexBaseQty, CexOrderRequest, ClientOrderId,
    ConnectionStatus, EngineParams, Exchange, ExecutionEvent, MarketConfig, MarketDataEvent,
    MarketDataSource, OrderRequest, OrderType, PolyOrderRequest, Price, RiskManager, Settings,
    SymbolRegistry, TimeInForce, TokenSide, create_channels,
};
use polyalpha_data::{
    CexBookLevel, CexBookUpdate, DataManager, MarketDataNormalizer, MockMarketDataSource, MockTick,
    PolyBookLevel, PolyBookUpdate,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::{DryRunExecutor, ExecutionManager};
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

#[derive(Parser, Debug)]
#[command(name = "polyalpha-cli", about = "PolyAlpha MVP CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    CheckConfig {
        #[arg(long, default_value = "default")]
        env: String,
    },
    Demo {
        #[arg(long, default_value = "default")]
        env: String,
    },
}

#[derive(Default)]
struct DemoStats {
    market_events: usize,
    signals_seen: usize,
    signals_rejected: usize,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::CheckConfig { env } => run_check_config(&env)?,
        Command::Demo { env } => run_demo(&env).await?,
    }

    Ok(())
}

fn run_check_config(env: &str) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;

    println!("env: {env}");
    println!("markets: {}", settings.markets.len());
    for market in &settings.markets {
        println!(
            "- {} -> {:?}/{} settlement={}",
            market.symbol.0, market.hedge_exchange, market.cex_symbol, market.settlement_timestamp
        );
    }

    Ok(())
}

async fn run_demo(env: &str) -> Result<()> {
    let settings = Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let market = settings
        .markets
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("config must contain at least one market"))?;

    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = DataManager::new(
        MarketDataNormalizer::new(registry),
        channels.market_data_tx.clone(),
    );

    let ticks = scripted_ticks(&market);
    let mut source = MockMarketDataSource::new(
        manager,
        Exchange::Polymarket,
        settings.strategy.settlement.clone(),
        ticks,
    );
    source.connect().await?;
    source.subscribe_market(&market.symbol).await?;

    let mut market_data_rx = channels.market_data_tx.subscribe();
    let mut engine = SimpleAlphaEngine::new(SimpleEngineConfig {
        warmup_samples: 1,
        basis_entry_threshold: 0.05,
        basis_exit_threshold: 0.02,
        ..SimpleEngineConfig::default()
    });
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut execution = ExecutionManager::new(DryRunExecutor::new());
    let mut stats = DemoStats::default();

    // 这里按 tick 顺序推进，保证 dry-run 的结果稳定可复现。
    while source.emit_next()?.is_some() {
        loop {
            match market_data_rx.try_recv() {
                Ok(event) => {
                    stats.market_events += 1;
                    if let MarketDataEvent::MarketLifecycle { symbol, phase, .. } = &event {
                        risk.update_market_phase(symbol.clone(), phase.clone());
                    }

                    let output = engine.on_market_data(&event).await;
                    for update in output.dmm_updates {
                        let events = execution.apply_dmm_quote_update(update).await?;
                        apply_execution_events(&mut risk, &mut stats, events).await?;
                    }

                    for signal in output.arb_signals {
                        stats.signals_seen += 1;

                        // 这里先在 CLI 做最小 pre-trade gate，避免 demo 把风控旁路掉。
                        if !signal_allowed(&risk, &signal).await? {
                            stats.signals_rejected += 1;
                            continue;
                        }

                        let events = execution.process_arb_signal(signal).await?;
                        apply_execution_events(&mut risk, &mut stats, events).await?;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Lagged(_)) => continue,
                Err(TryRecvError::Closed) => break,
            }
        }
    }

    let snapshot = risk.build_snapshot(1_800_000_000_000);
    println!("demo market: {}", market.symbol.0);
    println!("market events: {}", stats.market_events);
    println!(
        "signals seen/rejected: {}/{}",
        stats.signals_seen, stats.signals_rejected
    );
    println!(
        "orders submitted/cancelled/fills/state changes: {}/{}/{}/{}",
        stats.order_submitted, stats.order_cancelled, stats.fills, stats.state_changes
    );
    println!("final exposure usd: {}", snapshot.total_exposure_usd.0);
    println!("final daily pnl usd: {}", snapshot.daily_pnl.0);
    println!("final breaker: {:?}", snapshot.circuit_breaker);
    println!("final positions: {}", snapshot.positions.len());

    Ok(())
}

async fn signal_allowed(risk: &InMemoryRiskManager, signal: &ArbSignalEvent) -> Result<bool> {
    for request in signal_requests(signal) {
        if risk.pre_trade_check(request).await.is_err() {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn apply_execution_events(
    risk: &mut InMemoryRiskManager,
    stats: &mut DemoStats,
    events: Vec<ExecutionEvent>,
) -> Result<()> {
    for event in events {
        match event {
            ExecutionEvent::OrderSubmitted { .. } => {
                stats.order_submitted += 1;
            }
            ExecutionEvent::OrderFilled(fill) => {
                stats.fills += 1;
                risk.on_fill(&fill).await?;
            }
            ExecutionEvent::OrderCancelled { .. } => {
                stats.order_cancelled += 1;
            }
            ExecutionEvent::HedgeStateChanged { .. } => {
                stats.state_changes += 1;
            }
            ExecutionEvent::ReconcileRequired { .. } => {}
        }
    }

    Ok(())
}

fn signal_requests(signal: &ArbSignalEvent) -> Vec<OrderRequest> {
    match &signal.action {
        ArbSignalAction::BasisLong {
            poly_side,
            poly_target_shares,
            poly_target_notional,
            cex_side,
            cex_hedge_qty,
            ..
        }
        | ArbSignalAction::BasisShort {
            poly_side,
            poly_target_shares,
            poly_target_notional,
            cex_side,
            cex_hedge_qty,
            ..
        } => vec![
            OrderRequest::Poly(PolyOrderRequest {
                client_order_id: ClientOrderId("risk-poly".to_owned()),
                symbol: signal.symbol.clone(),
                token_side: TokenSide::Yes,
                side: *poly_side,
                order_type: OrderType::Market,
                limit_price: None,
                shares: Some(*poly_target_shares),
                quote_notional: Some(*poly_target_notional),
                time_in_force: TimeInForce::Fok,
                post_only: false,
            }),
            OrderRequest::Cex(CexOrderRequest {
                client_order_id: ClientOrderId("risk-cex".to_owned()),
                exchange: Exchange::Binance,
                symbol: signal.symbol.clone(),
                venue_symbol: venue_symbol(&signal.symbol),
                side: *cex_side,
                order_type: OrderType::Market,
                price: None,
                base_qty: *cex_hedge_qty,
                time_in_force: TimeInForce::Ioc,
                reduce_only: false,
            }),
        ],
        ArbSignalAction::DeltaRebalance {
            cex_side,
            cex_qty_adjust,
            ..
        } => vec![OrderRequest::Cex(CexOrderRequest {
            client_order_id: ClientOrderId("risk-rebalance".to_owned()),
            exchange: Exchange::Binance,
            symbol: signal.symbol.clone(),
            venue_symbol: venue_symbol(&signal.symbol),
            side: *cex_side,
            order_type: OrderType::Market,
            price: None,
            base_qty: *cex_qty_adjust,
            time_in_force: TimeInForce::Ioc,
            reduce_only: false,
        })],
        ArbSignalAction::NegRiskArb { legs } => legs
            .iter()
            .enumerate()
            .map(|(idx, leg)| {
                OrderRequest::Poly(PolyOrderRequest {
                    client_order_id: ClientOrderId(format!("risk-leg-{idx}")),
                    symbol: leg.symbol.clone(),
                    token_side: leg.token_side,
                    side: leg.side,
                    order_type: OrderType::Market,
                    limit_price: None,
                    shares: Some(leg.quantity),
                    quote_notional: None,
                    time_in_force: TimeInForce::Fok,
                    post_only: false,
                })
            })
            .collect(),
        ArbSignalAction::ClosePosition { .. } => Vec::new(),
    }
}

fn venue_symbol(symbol: &polyalpha_core::Symbol) -> String {
    symbol.0.to_ascii_uppercase().replace('-', "")
}

fn scripted_ticks(market: &MarketConfig) -> Vec<MockTick> {
    let symbol = market.symbol.clone();
    let trading_now = market.settlement_timestamp.saturating_sub(48 * 3600);
    let close_only_now = market.settlement_timestamp.saturating_sub(1800);

    vec![
        MockTick::Connection {
            exchange: Exchange::Polymarket,
            status: ConnectionStatus::Connected,
        },
        MockTick::Connection {
            exchange: market.hedge_exchange,
            status: ConnectionStatus::Connected,
        },
        MockTick::Lifecycle {
            symbol: symbol.clone(),
            now_timestamp_secs: trading_now,
            emitted_at_ms: 1,
        },
        MockTick::CexOrderBook(CexBookUpdate {
            exchange: market.hedge_exchange,
            venue_symbol: market.cex_symbol.clone(),
            bids: vec![CexBookLevel {
                price: Price(Decimal::new(100_000, 0)),
                base_qty: CexBaseQty(Decimal::new(5, 1)),
            }],
            asks: vec![CexBookLevel {
                price: Price(Decimal::new(100_010, 0)),
                base_qty: CexBaseQty(Decimal::new(5, 1)),
            }],
            exchange_timestamp_ms: 2,
            received_at_ms: 2,
            sequence: 2,
        }),
        MockTick::PolyOrderBook(PolyBookUpdate {
            asset_id: market.poly_ids.yes_token_id.clone(),
            bids: vec![PolyBookLevel {
                price: Price(Decimal::new(70, 2)),
                shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
            }],
            asks: vec![PolyBookLevel {
                price: Price(Decimal::new(72, 2)),
                shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
            }],
            exchange_timestamp_ms: 3,
            received_at_ms: 3,
            sequence: 3,
        }),
        MockTick::PolyOrderBook(PolyBookUpdate {
            asset_id: market.poly_ids.yes_token_id.clone(),
            bids: vec![PolyBookLevel {
                price: Price(Decimal::new(71, 2)),
                shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
            }],
            asks: vec![PolyBookLevel {
                price: Price(Decimal::new(73, 2)),
                shares: polyalpha_core::PolyShares(Decimal::new(25, 0)),
            }],
            exchange_timestamp_ms: 4,
            received_at_ms: 4,
            sequence: 4,
        }),
        MockTick::Lifecycle {
            symbol,
            now_timestamp_secs: close_only_now,
            emitted_at_ms: 5,
        },
    ]
}
