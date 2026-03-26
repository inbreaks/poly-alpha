use anyhow::{anyhow, Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio::sync::broadcast::error::TryRecvError;

use polyalpha_core::{
    cex_venue_symbol, create_channels, AlphaEngine, ArbSignalAction, ArbSignalEvent, CexBaseQty,
    CexOrderRequest, ClientOrderId, ConnectionStatus, EngineParams, Exchange, ExecutionEvent,
    MarketConfig, MarketDataEvent, MarketDataSource, OrderRequest, OrderType, PolyOrderRequest,
    Price, RiskManager, Settings, SymbolRegistry, TimeInForce, TokenSide,
};
use polyalpha_data::{
    BinanceFuturesDataSource, CexBookLevel, CexBookUpdate, DataManager, MarketDataNormalizer,
    MockMarketDataSource, MockTick, OkxMarketDataSource, PolyBookLevel, PolyBookUpdate,
    PolymarketLiveDataSource,
};
use polyalpha_engine::{SimpleAlphaEngine, SimpleEngineConfig};
use polyalpha_executor::{BinanceFuturesExecutor, DryRunExecutor, ExecutionManager, OkxExecutor};
use polyalpha_risk::{InMemoryRiskManager, RiskLimits};

use crate::args::{PreviewExchange, PreviewOrderType, PreviewSide};

#[derive(Default)]
struct DemoStats {
    market_events: usize,
    signals_seen: usize,
    signals_rejected: usize,
    order_submitted: usize,
    order_cancelled: usize,
    fills: usize,
    state_changes: usize,
    trades_closed: usize,
    total_pnl_usd: f64,
}

pub fn run_check_config(env: &str) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;

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

pub async fn run_demo(env: &str) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let market = select_market(&settings, 0)?;

    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = DataManager::new(
        MarketDataNormalizer::new(registry.clone()),
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
    let mut engine = SimpleAlphaEngine::with_markets(
        SimpleEngineConfig {
            min_signal_samples: 2,
            rolling_window_minutes: 4,
            entry_z: 2.0,
            exit_z: 0.5,
            position_notional_usd: settings.strategy.basis.max_position_usd,
            cex_hedge_ratio: 1.0,
            dmm_half_spread: Decimal::new(1, 2),
            dmm_quote_size: polyalpha_core::PolyShares::ZERO,
            ..SimpleEngineConfig::default()
        },
        settings.markets.clone(),
    );
    engine.update_params(EngineParams {
        basis_entry_zscore: None,
        basis_exit_zscore: None,
        rolling_window_secs: None,
        max_position_usd: Some(settings.strategy.basis.max_position_usd),
    });

    let mut risk = InMemoryRiskManager::new(RiskLimits::from(settings.risk.clone()));
    let mut execution =
        ExecutionManager::with_symbol_registry(DryRunExecutor::new(), registry.clone());
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
                        if !signal_allowed(&risk, &registry, &signal).await? {
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

pub async fn run_live_data_check(env: &str, market_index: usize, depth: u16) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let market = select_market(&settings, market_index)?;

    let registry = SymbolRegistry::new(settings.markets.clone());
    let channels = create_channels(std::slice::from_ref(&market.symbol));
    let manager = DataManager::new(
        MarketDataNormalizer::new(registry),
        channels.market_data_tx.clone(),
    );

    let mut poly = PolymarketLiveDataSource::new(
        manager.clone(),
        settings.polymarket.clob_api_url.clone(),
        settings.strategy.settlement.clone(),
    );
    let mut binance =
        BinanceFuturesDataSource::new(manager.clone(), settings.binance.rest_url.clone());
    let mut okx = OkxMarketDataSource::new(manager, settings.okx.rest_url.clone());

    poly.connect().await?;
    binance.connect().await?;
    okx.connect().await?;
    poly.subscribe_market(&market.symbol).await?;
    binance.subscribe_market(&market.symbol).await?;
    okx.subscribe_market(&market.symbol).await?;

    println!("market: {}", market.symbol.0);
    println!("hedge symbol: {}", market.cex_symbol);

    // 默认配置里的 token_id 还是占位值时，直接跳过真实 Poly 请求，避免误报。
    if looks_like_placeholder_id(&market.poly_ids.yes_token_id) {
        println!("polymarket: skipped, yes_token_id is still a placeholder");
        println!(
            "hint: run `polyalpha-cli markets discover-btc --match-text 100k --pick 0 --output config/live.auto.toml` to generate a live market overlay"
        );
    } else {
        match poly
            .fetch_orderbook_by_symbol(&market.symbol, TokenSide::Yes)
            .await
        {
            Ok(poly_book) => {
                println!(
                    "polymarket yes book: best_bid={} best_ask={} bids={} asks={}",
                    best_poly_bid(&poly_book).unwrap_or_else(|| "n/a".to_owned()),
                    best_poly_ask(&poly_book).unwrap_or_else(|| "n/a".to_owned()),
                    poly_book.bids.len(),
                    poly_book.asks.len(),
                );
            }
            Err(err) => {
                println!("polymarket: error={err}");
            }
        }
    }

    match binance.fetch_orderbook(&market.cex_symbol, depth).await {
        Ok(binance_book) => match binance.fetch_funding_and_mark(&market.cex_symbol).await {
            Ok((binance_funding, binance_mark)) => {
                println!(
                    "binance: best_bid={} best_ask={} funding={} mark={}",
                    best_cex_bid(&binance_book).unwrap_or_else(|| "n/a".to_owned()),
                    best_cex_ask(&binance_book).unwrap_or_else(|| "n/a".to_owned()),
                    binance_funding.rate,
                    binance_mark.price.0,
                );
            }
            Err(err) => {
                println!("binance: funding/mark error={err}");
            }
        },
        Err(err) => {
            println!("binance: orderbook error={err}");
        }
    }

    match okx.fetch_orderbook(&market.cex_symbol, depth).await {
        Ok(okx_book) => match okx.fetch_funding_and_mark(&market.cex_symbol).await {
            Ok((okx_funding, okx_mark)) => {
                println!(
                    "okx: best_bid={} best_ask={} funding={} mark={}",
                    best_cex_bid(&okx_book).unwrap_or_else(|| "n/a".to_owned()),
                    best_cex_ask(&okx_book).unwrap_or_else(|| "n/a".to_owned()),
                    okx_funding.rate,
                    okx_mark.price.0,
                );
            }
            Err(err) => {
                println!("okx: funding/mark error={err}");
            }
        },
        Err(err) => {
            println!("okx: orderbook error={err}");
        }
    }

    println!(
        "ws subscribe samples: poly={} binance={} okx={}",
        PolymarketLiveDataSource::build_orderbook_subscribe_message(&[market
            .poly_ids
            .yes_token_id
            .clone(),]),
        BinanceFuturesDataSource::build_depth_subscribe_message(&market.cex_symbol, 1),
        OkxMarketDataSource::build_books_subscribe_message(&cex_venue_symbol(
            Exchange::Okx,
            &market.cex_symbol,
        )),
    );

    Ok(())
}

pub async fn run_live_exec_preview(
    env: &str,
    market_index: usize,
    exchange: PreviewExchange,
    side: PreviewSide,
    order_type: PreviewOrderType,
    qty: &str,
    price: Option<&str>,
    reduce_only: bool,
) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let market = select_market(&settings, market_index)?;
    let base_qty = parse_decimal_arg("qty", qty)?;
    let limit_price = match price {
        Some(raw) => Some(Price(parse_decimal_arg("price", raw)?)),
        None => None,
    };

    let request = build_preview_order(
        &market,
        exchange,
        side,
        order_type,
        CexBaseQty(base_qty),
        limit_price,
        reduce_only,
    )?;

    match exchange {
        PreviewExchange::Binance => {
            let api_key = required_env("BINANCE_API_KEY")?;
            let api_secret = required_env("BINANCE_API_SECRET")?;
            let executor =
                BinanceFuturesExecutor::new(settings.binance.rest_url.clone(), api_key, api_secret);
            let preview = executor.preview_submit_order(&request, 1_700_000_000_000)?;
            println!("exchange: binance");
            println!("method: {}", preview.method);
            println!("path: {}", preview.path);
            println!("query: {}", preview.query);
            println!("url: {}", preview.url);
        }
        PreviewExchange::Okx => {
            let api_key = required_env("OKX_API_KEY")?;
            let api_secret = required_env("OKX_API_SECRET")?;
            let passphrase = required_env("OKX_PASSPHRASE")?;
            let executor = OkxExecutor::new(
                settings.okx.rest_url.clone(),
                api_key,
                api_secret,
                passphrase,
            );
            let preview = executor.preview_submit_order(&request, "2026-03-17T00:00:00.000Z")?;
            println!("exchange: okx");
            println!("method: {}", preview.method);
            println!("path: {}", preview.path);
            println!("timestamp: {}", preview.timestamp);
            println!("signature: {}", preview.signature);
            println!("body: {}", preview.body);
        }
    }

    Ok(())
}

pub(crate) fn select_market(settings: &Settings, market_index: usize) -> Result<MarketConfig> {
    settings
        .markets
        .get(market_index)
        .cloned()
        .ok_or_else(|| anyhow!("config does not contain market index {}", market_index))
}

pub(crate) async fn signal_allowed(
    risk: &InMemoryRiskManager,
    registry: &SymbolRegistry,
    signal: &ArbSignalEvent,
) -> Result<bool> {
    Ok(signal_rejection_reason(risk, registry, signal)
        .await?
        .is_none())
}

pub(crate) async fn signal_rejection_reason(
    risk: &InMemoryRiskManager,
    registry: &SymbolRegistry,
    signal: &ArbSignalEvent,
) -> Result<Option<String>> {
    for request in signal_requests(registry, signal)? {
        if let Err(err) = risk.pre_trade_check(request).await {
            return Ok(Some(err.to_string()));
        }
    }

    Ok(None)
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
            ExecutionEvent::TradeClosed { realized_pnl, .. } => {
                stats.trades_closed += 1;
                stats.total_pnl_usd += realized_pnl.0.to_f64().unwrap_or(0.0);
            }
        }
    }

    Ok(())
}

fn signal_requests(
    registry: &SymbolRegistry,
    signal: &ArbSignalEvent,
) -> Result<Vec<OrderRequest>> {
    let requests = match &signal.action {
        ArbSignalAction::BasisLong {
            token_side,
            poly_side,
            poly_target_shares,
            poly_target_notional,
            cex_side,
            cex_hedge_qty,
            ..
        }
        | ArbSignalAction::BasisShort {
            token_side,
            poly_side,
            poly_target_shares,
            poly_target_notional,
            cex_side,
            cex_hedge_qty,
            ..
        } => {
            let market = registry
                .get_config(&signal.symbol)
                .ok_or_else(|| anyhow!("missing market config for symbol {}", signal.symbol.0))?;
            vec![
                OrderRequest::Poly(PolyOrderRequest {
                    client_order_id: ClientOrderId("risk-poly".to_owned()),
                    symbol: signal.symbol.clone(),
                    token_side: *token_side,
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
                    exchange: market.hedge_exchange,
                    symbol: signal.symbol.clone(),
                    venue_symbol: cex_venue_symbol(market.hedge_exchange, &market.cex_symbol),
                    side: *cex_side,
                    order_type: OrderType::Market,
                    price: None,
                    base_qty: *cex_hedge_qty,
                    time_in_force: TimeInForce::Ioc,
                    reduce_only: false,
                }),
            ]
        }
        ArbSignalAction::DeltaRebalance {
            cex_side,
            cex_qty_adjust,
            ..
        } => {
            let market = registry
                .get_config(&signal.symbol)
                .ok_or_else(|| anyhow!("missing market config for symbol {}", signal.symbol.0))?;
            vec![OrderRequest::Cex(CexOrderRequest {
                client_order_id: ClientOrderId("risk-rebalance".to_owned()),
                exchange: market.hedge_exchange,
                symbol: signal.symbol.clone(),
                venue_symbol: cex_venue_symbol(market.hedge_exchange, &market.cex_symbol),
                side: *cex_side,
                order_type: OrderType::Market,
                price: None,
                base_qty: *cex_qty_adjust,
                time_in_force: TimeInForce::Ioc,
                reduce_only: false,
            })]
        }
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
    };

    Ok(requests)
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required environment variable `{name}`"))
}

fn parse_decimal_arg(label: &str, raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).with_context(|| format!("invalid decimal for `{label}`: {raw}"))
}

fn build_preview_order(
    market: &MarketConfig,
    exchange: PreviewExchange,
    side: PreviewSide,
    order_type: PreviewOrderType,
    base_qty: CexBaseQty,
    price: Option<Price>,
    reduce_only: bool,
) -> Result<CexOrderRequest> {
    let exchange_value = match exchange {
        PreviewExchange::Binance => Exchange::Binance,
        PreviewExchange::Okx => Exchange::Okx,
    };
    let side_value = match side {
        PreviewSide::Buy => polyalpha_core::OrderSide::Buy,
        PreviewSide::Sell => polyalpha_core::OrderSide::Sell,
    };
    let (order_type_value, tif) = match order_type {
        PreviewOrderType::Market => (OrderType::Market, TimeInForce::Ioc),
        PreviewOrderType::Limit => (OrderType::Limit, TimeInForce::Gtc),
        PreviewOrderType::PostOnly => (OrderType::PostOnly, TimeInForce::Gtc),
    };

    if matches!(
        order_type,
        PreviewOrderType::Limit | PreviewOrderType::PostOnly
    ) && price.is_none()
    {
        return Err(anyhow!("limit/post-only preview requires `--price`"));
    }

    Ok(CexOrderRequest {
        client_order_id: ClientOrderId("preview-order".to_owned()),
        exchange: exchange_value,
        symbol: market.symbol.clone(),
        venue_symbol: cex_venue_symbol(exchange_value, &market.cex_symbol),
        side: side_value,
        order_type: order_type_value,
        price,
        base_qty,
        time_in_force: tif,
        reduce_only,
    })
}

fn looks_like_placeholder_id(value: &str) -> bool {
    value.trim().is_empty() || value.contains("...")
}

fn best_poly_bid(update: &PolyBookUpdate) -> Option<String> {
    update
        .bids
        .first()
        .map(|level| level.price.0.normalize().to_string())
}

fn best_poly_ask(update: &PolyBookUpdate) -> Option<String> {
    update
        .asks
        .first()
        .map(|level| level.price.0.normalize().to_string())
}

fn best_cex_bid(update: &CexBookUpdate) -> Option<String> {
    update
        .bids
        .first()
        .map(|level| level.price.0.normalize().to_string())
}

fn best_cex_ask(update: &CexBookUpdate) -> Option<String> {
    update
        .asks
        .first()
        .map(|level| level.price.0.normalize().to_string())
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
            last_trade_price: None,
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
            last_trade_price: None,
        }),
        MockTick::Lifecycle {
            symbol,
            now_timestamp_secs: close_only_now,
            emitted_at_ms: 5,
        },
    ]
}
