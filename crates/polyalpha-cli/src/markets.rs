use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::StreamExt;
use regex::Regex;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;

use polyalpha_core::{
    Exchange, MarketConfig, MarketRule, MarketRuleKind, Price, Settings, StrategyMarketScopeConfig,
    Symbol,
};

use crate::args::MarketAsset;
use crate::commands::select_market;
use crate::strategy_scope::{
    looks_like_any_price_market_text, main_strategy_rejection_reason,
    parse_supported_terminal_market_rule, strategy_rejection_label_zh,
};

const GAMMA_EVENTS_URL: &str = "https://gamma-api.polymarket.com/events";
const GAMMA_PUBLIC_SEARCH_URL: &str = "https://gamma-api.polymarket.com/public-search";
const BINANCE_EXCHANGE_INFO_PATH: &str = "/fapi/v1/exchangeInfo";
const MAX_PUBLIC_SEARCH_PAGES: usize = 10;
const EVENT_FETCH_CONCURRENCY: usize = 12;
const HTTP_GET_RETRY_ATTEMPTS: usize = 3;
const HTTP_GET_RETRY_BACKOFF_MS: u64 = 400;
const CLOB_BOOK_PROBE_CONCURRENCY: usize = 24;
const CLOB_BOOK_PROBE_RETRY_ATTEMPTS: usize = 2;
const CLOB_BOOK_PROBE_RETRY_BACKOFF_MS: u64 = 250;
const DISCOVERY_WARNING_SAMPLE_LIMIT: usize = 20;
const BTC_KEYWORDS: [&str; 2] = ["btc", "bitcoin"];
const ETH_KEYWORDS: [&str; 2] = ["eth", "ethereum"];
const SOL_KEYWORDS: [&str; 1] = ["solana"];
const XRP_KEYWORDS: [&str; 2] = ["xrp", "ripple"];
const BTC_SEARCH_QUERIES: [&str; 2] = ["bitcoin", "btc"];
const ETH_SEARCH_QUERIES: [&str; 1] = ["ethereum"];
const SOL_SEARCH_QUERIES: [&str; 1] = ["solana"];
const XRP_SEARCH_QUERIES: [&str; 2] = ["xrp", "ripple"];
const MONEY_CAPTURE_RE: &str = r"([0-9][0-9,]*(?:\.\d+)?(?:[kmb])?)";

type CexQtyStepSyncMap = HashMap<(Exchange, String), Decimal>;

#[derive(Clone, Debug)]
struct DiscoveredMarket {
    event_id: String,
    event_title: String,
    event_slug: String,
    market_id: String,
    question: String,
    market_slug: String,
    condition_id: String,
    yes_token_id: String,
    no_token_id: String,
    settlement_timestamp: u64,
    settlement_iso: String,
    neg_risk: bool,
    strike_price: Option<Price>,
    market_rule: Option<MarketRule>,
    event_archived: bool,
    market_archived: bool,
    accepting_orders: bool,
    enable_orderbook: bool,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    liquidity_num: Option<f64>,
    volume_num: Option<f64>,
    volume_24h: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
struct AssetSpec {
    asset: MarketAsset,
    display_name: &'static str,
    default_query: &'static str,
    keywords: &'static [&'static str],
    cex_symbol: &'static str,
}

#[derive(Clone, Debug, Default)]
struct RefreshAssetSummary {
    display_name: &'static str,
    discovered: usize,
    static_validated: usize,
    validated: usize,
    selected: usize,
    matched_catalog: usize,
    synthesized: usize,
    skipped_static_validation: usize,
    skipped_probe_validation: usize,
    skipped_generation: usize,
    warnings: usize,
}

#[derive(Clone, Copy, Debug)]
struct MarketValidationPolicy {
    max_markets_per_asset: usize,
    min_minutes_to_settlement: i64,
    min_liquidity_usd: f64,
    min_volume_24h_usd: f64,
    market_scope: StrategyMarketScopeConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ValidationRejectionReason {
    EventArchived,
    MarketArchived,
    NotAcceptingOrders,
    OrderBookDisabled,
    Settled,
    SettlesTooSoon,
    PathDependentMarketNotSupported,
    AllTimeHighMarketNotSupported,
    NonPriceMarketNotSupported,
    TerminalPriceMarketNotSupported,
    MissingTradabilitySignal,
    NoClobOrderbook,
    ClobProbeFailed,
}

impl ValidationRejectionReason {
    fn code(self) -> &'static str {
        match self {
            Self::EventArchived => "event_archived",
            Self::MarketArchived => "market_archived",
            Self::NotAcceptingOrders => "not_accepting_orders",
            Self::OrderBookDisabled => "orderbook_disabled",
            Self::Settled => "settled",
            Self::SettlesTooSoon => "settles_too_soon",
            Self::PathDependentMarketNotSupported => "path_dependent_market_not_supported",
            Self::AllTimeHighMarketNotSupported => "all_time_high_market_not_supported",
            Self::NonPriceMarketNotSupported => "non_price_market_not_supported",
            Self::TerminalPriceMarketNotSupported => "terminal_price_market_not_supported",
            Self::MissingTradabilitySignal => "missing_tradability_signal",
            Self::NoClobOrderbook => "no_clob_orderbook",
            Self::ClobProbeFailed => "clob_probe_failed",
        }
    }

    fn label_zh(self) -> &'static str {
        match self {
            Self::EventArchived => "事件已归档",
            Self::MarketArchived => "市场已归档",
            Self::NotAcceptingOrders => "市场不再接单",
            Self::OrderBookDisabled => "订单簿未启用",
            Self::Settled => "市场已结算/到期",
            Self::SettlesTooSoon => "距离结算过近",
            Self::PathDependentMarketNotSupported => {
                strategy_rejection_label_zh("path_dependent_market_not_supported")
            }
            Self::AllTimeHighMarketNotSupported => {
                strategy_rejection_label_zh("all_time_high_market_not_supported")
            }
            Self::NonPriceMarketNotSupported => {
                strategy_rejection_label_zh("non_price_market_not_supported")
            }
            Self::TerminalPriceMarketNotSupported => {
                strategy_rejection_label_zh("terminal_price_market_not_supported")
            }
            Self::MissingTradabilitySignal => "缺少流动性/成交活跃信号",
            Self::NoClobOrderbook => "CLOB 无可用订单簿",
            Self::ClobProbeFailed => "CLOB 探测失败",
        }
    }
}

fn validation_reason_from_code(code: &str) -> Option<ValidationRejectionReason> {
    match code {
        "event_archived" => Some(ValidationRejectionReason::EventArchived),
        "market_archived" => Some(ValidationRejectionReason::MarketArchived),
        "not_accepting_orders" => Some(ValidationRejectionReason::NotAcceptingOrders),
        "orderbook_disabled" => Some(ValidationRejectionReason::OrderBookDisabled),
        "settled" => Some(ValidationRejectionReason::Settled),
        "settles_too_soon" => Some(ValidationRejectionReason::SettlesTooSoon),
        "path_dependent_market_not_supported" => {
            Some(ValidationRejectionReason::PathDependentMarketNotSupported)
        }
        "all_time_high_market_not_supported" => {
            Some(ValidationRejectionReason::AllTimeHighMarketNotSupported)
        }
        "non_price_market_not_supported" => {
            Some(ValidationRejectionReason::NonPriceMarketNotSupported)
        }
        "terminal_price_market_not_supported" => {
            Some(ValidationRejectionReason::TerminalPriceMarketNotSupported)
        }
        "missing_tradability_signal" => Some(ValidationRejectionReason::MissingTradabilitySignal),
        "no_clob_orderbook" => Some(ValidationRejectionReason::NoClobOrderbook),
        "clob_probe_failed" => Some(ValidationRejectionReason::ClobProbeFailed),
        _ => None,
    }
}

#[derive(Debug)]
struct ValidationSelection {
    ranked_validated: Vec<DiscoveredMarket>,
    validated_total: usize,
    target_selected: usize,
    truncated: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CandidateLifecycleStatus {
    Rejected,
    ValidatedNotSelected,
    GenerationFailed,
    Selected,
}

impl CandidateLifecycleStatus {
    fn code(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::ValidatedNotSelected => "validated_not_selected",
            Self::GenerationFailed => "generation_failed",
            Self::Selected => "selected",
        }
    }

    fn label_zh(self) -> &'static str {
        match self {
            Self::Rejected => "校验拒绝",
            Self::ValidatedNotSelected => "已通过校验但未入选",
            Self::GenerationFailed => "通过校验但生成配置失败",
            Self::Selected => "已入选运行集",
        }
    }
}

fn candidate_lifecycle_status_from_code(code: &str) -> Option<CandidateLifecycleStatus> {
    match code {
        "rejected" => Some(CandidateLifecycleStatus::Rejected),
        "validated_not_selected" => Some(CandidateLifecycleStatus::ValidatedNotSelected),
        "generation_failed" => Some(CandidateLifecycleStatus::GenerationFailed),
        "selected" => Some(CandidateLifecycleStatus::Selected),
        _ => None,
    }
}

#[derive(Debug)]
struct RefreshAssetArtifact {
    summary: RefreshAssetSummary,
    warnings: Vec<String>,
    report: RefreshAssetReport,
    generated_markets: Vec<MarketConfig>,
}

#[derive(Debug, Serialize)]
struct RefreshActiveReport {
    report_version: u8,
    generated_at_utc: String,
    base_env: String,
    catalog_env: String,
    output_toml: String,
    validation: RefreshValidationPolicyReport,
    totals: RefreshReportTotals,
    assets: Vec<RefreshAssetReport>,
}

#[derive(Debug, Serialize)]
struct RefreshValidationPolicyReport {
    max_markets_per_asset: usize,
    min_minutes_to_settlement: i64,
    min_liquidity_usd: f64,
    min_volume_24h_usd: f64,
    clob_book_probe_enabled: bool,
}

#[derive(Debug, Serialize)]
struct RefreshReportTotals {
    discovered: usize,
    static_validated: usize,
    validated: usize,
    selected: usize,
    skipped_static_validation: usize,
    skipped_probe_validation: usize,
    skipped_generation: usize,
    rejected_reason_counts: Vec<RefreshCountEntry>,
    lifecycle_status_counts: Vec<RefreshCountEntry>,
}

#[derive(Debug, Serialize)]
struct RefreshAssetReport {
    asset: String,
    discovered: usize,
    static_validated: usize,
    validated: usize,
    selection_target: usize,
    selected: usize,
    truncated: usize,
    catalog_match: usize,
    synthesized: usize,
    skipped_static_validation: usize,
    skipped_probe_validation: usize,
    skipped_generation: usize,
    discovery_warning_count: usize,
    discovery_warning_samples: Vec<String>,
    warnings: usize,
    rejected_reason_counts: Vec<RefreshCountEntry>,
    lifecycle_status_counts: Vec<RefreshCountEntry>,
    candidates: Vec<RefreshCandidateReport>,
}

#[derive(Debug, Serialize)]
struct RefreshCountEntry {
    code: String,
    label_zh: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct RefreshCandidateReport {
    market_slug: String,
    market_id: String,
    condition_id: String,
    event_id: String,
    event_title: String,
    event_slug: String,
    question: String,
    settlement_utc: String,
    settlement_timestamp: u64,
    rank: Option<usize>,
    probe_checked: bool,
    lifecycle_status: String,
    lifecycle_status_zh: String,
    output_symbol: Option<String>,
    rejection_reason: Option<String>,
    rejection_reason_zh: Option<String>,
    generation_error: Option<String>,
    runtime_selected: bool,
    runtime_source: Option<String>,
    catalog_available: bool,
    accepting_orders: bool,
    orderbook_enabled: bool,
    event_archived: bool,
    market_archived: bool,
    has_live_bbo: bool,
    has_activity_signal: bool,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    liquidity_usd: Option<f64>,
    volume_total_usd: Option<f64>,
    volume_24h_usd: Option<f64>,
}

impl AssetSpec {
    fn from_asset(asset: MarketAsset) -> Self {
        match asset {
            MarketAsset::Btc => Self {
                asset,
                display_name: "BTC",
                default_query: "bitcoin",
                keywords: &BTC_KEYWORDS,
                cex_symbol: "BTCUSDT",
            },
            MarketAsset::Eth => Self {
                asset,
                display_name: "ETH",
                default_query: "ethereum",
                keywords: &ETH_KEYWORDS,
                cex_symbol: "ETHUSDT",
            },
            MarketAsset::Sol => Self {
                asset,
                display_name: "SOL",
                default_query: "solana",
                keywords: &SOL_KEYWORDS,
                cex_symbol: "SOLUSDT",
            },
            MarketAsset::Xrp => Self {
                asset,
                display_name: "XRP",
                default_query: "xrp",
                keywords: &XRP_KEYWORDS,
                cex_symbol: "XRPUSDT",
            },
        }
    }

    fn command_suffix(self) -> &'static str {
        match self.asset {
            MarketAsset::Btc => "btc",
            MarketAsset::Eth => "eth",
            MarketAsset::Sol => "sol",
            MarketAsset::Xrp => "xrp",
        }
    }

    fn search_queries(self) -> &'static [&'static str] {
        match self.asset {
            MarketAsset::Btc => &BTC_SEARCH_QUERIES,
            MarketAsset::Eth => &ETH_SEARCH_QUERIES,
            MarketAsset::Sol => &SOL_SEARCH_QUERIES,
            MarketAsset::Xrp => &XRP_SEARCH_QUERIES,
        }
    }
}

pub async fn run_discover_btc(
    env: &str,
    template_market_index: Option<usize>,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    run_discover_asset(
        MarketAsset::Btc,
        env,
        template_market_index,
        query,
        match_text,
        limit,
        pick,
        output,
    )
    .await
}

pub async fn run_discover_eth(
    env: &str,
    template_market_index: Option<usize>,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    run_discover_asset(
        MarketAsset::Eth,
        env,
        template_market_index,
        query,
        match_text,
        limit,
        pick,
        output,
    )
    .await
}

pub async fn run_discover_sol(
    env: &str,
    template_market_index: Option<usize>,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    run_discover_asset(
        MarketAsset::Sol,
        env,
        template_market_index,
        query,
        match_text,
        limit,
        pick,
        output,
    )
    .await
}

pub async fn run_discover_xrp(
    env: &str,
    template_market_index: Option<usize>,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    run_discover_asset(
        MarketAsset::Xrp,
        env,
        template_market_index,
        query,
        match_text,
        limit,
        pick,
        output,
    )
    .await
}

async fn run_discover_asset(
    asset: MarketAsset,
    env: &str,
    template_market_index: Option<usize>,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    let asset = AssetSpec::from_asset(asset);
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let template = select_template_market(&settings, asset, template_market_index)?;
    let client = Client::builder()
        .user_agent("polyalpha-market-discovery/0.1")
        .build()
        .context("failed to build discovery http client")?;

    let (mut candidates, warnings) =
        fetch_active_asset_markets(&client, asset, query, limit.max(1)).await?;
    if let Some(text) = match_text {
        candidates.retain(|market| market_matches(market, text));
    }

    if candidates.is_empty() {
        return Err(anyhow!(
            "no active {} markets matched query `{query}` and match filter `{}`",
            asset.display_name,
            match_text.unwrap_or("")
        ));
    }

    if let Some(index) = pick {
        let selected = candidates.get(index).ok_or_else(|| {
            anyhow!(
                "pick index {} is out of range for {} discovered markets",
                index,
                candidates.len()
            )
        })?;
        let snippet = render_market_snippet(selected, &template, asset);
        println!("{snippet}");

        if let Some(path) = output {
            write_output(path, &snippet)?;
        }
    } else {
        print_candidates(&candidates, asset);
        println!(
            "hint: rerun with `--pick <index>` to render TOML, e.g. `polyalpha-cli markets discover-{} --pick 0 --output config/{}.auto.toml`",
            asset.command_suffix(),
            asset.command_suffix(),
        );
    }

    print_discovery_warnings(&warnings);
    Ok(())
}

pub async fn run_refresh_active(
    base_env: &str,
    catalog_env: &str,
    assets: &[MarketAsset],
    limit_per_asset: usize,
    max_markets_per_asset: usize,
    min_minutes_to_settlement: i64,
    min_liquidity_usd: f64,
    min_volume_24h_usd: f64,
    output: Option<&str>,
    report_json: Option<&str>,
) -> Result<()> {
    let base_settings = Settings::load(base_env)
        .with_context(|| format!("failed to load base config env `{base_env}`"))?;
    let catalog_settings = Settings::load(catalog_env)
        .with_context(|| format!("failed to load catalog config env `{catalog_env}`"))?;
    let client = Client::builder()
        .user_agent("polyalpha-market-discovery/0.1")
        .build()
        .context("failed to build discovery http client")?;
    let selected_assets = resolve_assets(assets);
    let validation_policy = MarketValidationPolicy {
        max_markets_per_asset: max_markets_per_asset.max(1),
        min_minutes_to_settlement,
        min_liquidity_usd: min_liquidity_usd.max(0.0),
        min_volume_24h_usd: min_volume_24h_usd.max(0.0),
        market_scope: base_settings.strategy.market_scope.clone(),
    };
    let output_path = output
        .map(str::to_owned)
        .unwrap_or_else(|| format!("config/{base_env}.toml"));
    let report_path = report_json
        .map(str::to_owned)
        .unwrap_or_else(|| derive_refresh_report_path(Path::new(&output_path)));

    let catalog_by_condition = catalog_settings
        .markets
        .iter()
        .cloned()
        .map(|market| (market.poly_ids.condition_id.clone(), market))
        .collect::<HashMap<_, _>>();

    let mut generated_markets = Vec::new();
    let mut seen_conditions = HashSet::new();
    let mut summaries = Vec::new();
    let mut asset_reports = Vec::new();
    let mut warnings = Vec::new();
    let mut templates = Vec::with_capacity(selected_assets.len());

    for asset in selected_assets.iter().copied() {
        let template = find_asset_template_market(&catalog_settings, asset)
            .or_else(|| find_asset_template_market(&base_settings, asset))
            .ok_or_else(|| {
                anyhow!(
                    "no template market found for {} in envs `{}` / `{}`",
                    asset.display_name,
                    catalog_env,
                    base_env
                )
            })?;
        templates.push((asset, template));
    }

    let synced_cex_qty_steps = fetch_binance_cex_qty_steps(
        &client,
        &base_settings.binance.rest_url,
        &templates
            .iter()
            .filter(|(_, template)| template.hedge_exchange == Exchange::Binance)
            .map(|(_, template)| template.cex_symbol.clone())
            .collect::<Vec<_>>(),
    )
    .await?;

    for (asset, template) in templates {
        let artifact = refresh_asset_markets(
            &client,
            &base_settings.polymarket.clob_api_url,
            asset,
            &template,
            &synced_cex_qty_steps,
            &catalog_by_condition,
            limit_per_asset.max(1),
            validation_policy,
        )
        .await?;
        warnings.extend(artifact.warnings.iter().cloned());

        for market in artifact.generated_markets.iter().cloned() {
            if seen_conditions.insert(market.poly_ids.condition_id.clone()) {
                generated_markets.push(market);
            } else {
                warnings.push(format!(
                    "{}: duplicated condition {} skipped during output merge",
                    asset.display_name, market.poly_ids.condition_id
                ));
            }
        }

        summaries.push(artifact.summary);
        asset_reports.push(artifact.report);
    }

    let mut seen_runtime_symbols = HashSet::new();
    for market in &generated_markets {
        if !seen_runtime_symbols.insert(market.symbol.0.clone()) {
            return Err(anyhow!(
                "duplicate runtime symbol generated after validation: {}",
                market.symbol.0
            ));
        }
    }

    generated_markets.sort_by(compare_markets_for_output);

    if generated_markets.is_empty() {
        return Err(anyhow!(
            "no active markets discovered for assets {:?}",
            selected_assets
                .iter()
                .map(|asset| asset.display_name)
                .collect::<Vec<_>>()
        ));
    }

    let mut output_settings = base_settings.clone();
    output_settings.markets = generated_markets;
    let payload = render_generated_settings(
        &output_settings,
        base_env,
        catalog_env,
        &selected_assets,
        &summaries,
        validation_policy,
    )?;
    write_output(&output_path, &payload)?;
    let report = build_refresh_report(
        base_env,
        catalog_env,
        &output_path,
        validation_policy,
        asset_reports,
    );
    let report_payload =
        serde_json::to_string_pretty(&report).context("failed to serialize refresh report")?;
    write_output(&report_path, &report_payload)?;

    println!("active market refresh summary:");
    for summary in &summaries {
        println!(
            "  {} discovered={} static_validated={} probe_validated={} selected={} catalog_match={} synthesized={} skipped_static_validation={} skipped_probe_validation={} skipped_generation={} warnings={}",
            summary.display_name,
            summary.discovered,
            summary.static_validated,
            summary.validated,
            summary.selected,
            summary.matched_catalog,
            summary.synthesized,
            summary.skipped_static_validation,
            summary.skipped_probe_validation,
            summary.skipped_generation,
            summary.warnings
        );
    }
    println!("  total markets written={}", output_settings.markets.len());
    println!("  validation report={report_path}");
    print_discovery_warnings(&warnings);

    Ok(())
}

async fn refresh_asset_markets(
    client: &Client,
    clob_api_url: &str,
    asset: AssetSpec,
    template: &MarketConfig,
    synced_cex_qty_steps: &CexQtyStepSyncMap,
    catalog_by_condition: &HashMap<String, MarketConfig>,
    limit_per_asset: usize,
    validation_policy: MarketValidationPolicy,
) -> Result<RefreshAssetArtifact> {
    let (discovered, asset_warnings) =
        fetch_active_asset_markets(client, asset, asset.default_query, limit_per_asset).await?;
    let asset_warning_count = asset_warnings.len();
    let discovery_warning_samples = asset_warnings
        .iter()
        .take(DISCOVERY_WARNING_SAMPLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>();
    let mut summary = RefreshAssetSummary {
        display_name: asset.display_name,
        discovered: discovered.len(),
        warnings: asset_warning_count,
        ..RefreshAssetSummary::default()
    };
    let mut warnings = asset_warnings
        .into_iter()
        .map(|warning| format!("{}: {warning}", asset.display_name))
        .collect::<Vec<_>>();

    let (static_selection, static_rejected) =
        validate_and_select_markets(discovered, validation_policy);
    summary.static_validated = static_selection.validated_total;
    summary.skipped_static_validation = static_rejected.len();
    warnings.extend(static_rejected.iter().map(|(market, reason)| {
        format!(
            "{}: filtered out {} - {}",
            asset.display_name,
            market.market_slug,
            reason.label_zh()
        )
    }));

    let probe_results =
        probe_discovered_markets(client, clob_api_url, static_selection.ranked_validated).await;
    let mut probe_validated = Vec::new();
    let mut probe_rejected = Vec::new();
    for (market, rejection) in probe_results {
        match rejection {
            Some(reason) => probe_rejected.push((market, reason)),
            None => probe_validated.push(market),
        }
    }
    summary.validated = probe_validated.len();
    summary.skipped_probe_validation = probe_rejected.len();
    warnings.extend(probe_rejected.iter().map(|(market, reason)| {
        format!(
            "{}: clob probe rejected {} - {}",
            asset.display_name,
            market.market_slug,
            reason.label_zh()
        )
    }));

    let selection = build_validation_selection(probe_validated, validation_policy);
    if selection.truncated > 0 {
        warnings.push(format!(
            "{}: truncated {} validated markets beyond top {} runtime slots",
            asset.display_name, selection.truncated, validation_policy.max_markets_per_asset
        ));
    }

    let mut candidates = Vec::with_capacity(
        static_rejected.len() + probe_rejected.len() + selection.ranked_validated.len(),
    );
    let mut candidate_index_by_condition = HashMap::new();

    for (market, reason) in &static_rejected {
        let index = candidates.len();
        candidates.push(build_candidate_report(
            market,
            validation_policy,
            None,
            false,
            CandidateLifecycleStatus::Rejected,
            Some(*reason),
            None,
            false,
            None,
        ));
        candidate_index_by_condition.insert(market.condition_id.clone(), index);
    }

    for (market, reason) in &probe_rejected {
        let index = candidates.len();
        candidates.push(build_candidate_report(
            market,
            validation_policy,
            None,
            true,
            CandidateLifecycleStatus::Rejected,
            Some(*reason),
            None,
            catalog_by_condition.contains_key(&market.condition_id),
            None,
        ));
        candidate_index_by_condition.insert(market.condition_id.clone(), index);
    }

    for (rank, market) in selection.ranked_validated.iter().enumerate() {
        let index = candidates.len();
        candidates.push(build_candidate_report(
            market,
            validation_policy,
            Some(rank + 1),
            true,
            CandidateLifecycleStatus::ValidatedNotSelected,
            None,
            None,
            catalog_by_condition.contains_key(&market.condition_id),
            None,
        ));
        candidate_index_by_condition.insert(market.condition_id.clone(), index);
    }

    let mut generated_markets = Vec::new();
    let mut selected_symbols = HashSet::new();
    for discovered_market in selection.ranked_validated {
        if summary.selected >= selection.target_selected {
            break;
        }

        let candidate_index = candidate_index_by_condition
            .get(&discovered_market.condition_id)
            .copied();
        let mut market = if let Some(existing) = catalog_by_condition
            .get(&discovered_market.condition_id)
            .cloned()
        {
            summary.matched_catalog += 1;
            let existing = apply_synced_cex_qty_step(
                refresh_catalog_market_config(existing, &discovered_market),
                synced_cex_qty_steps,
            );
            mark_candidate_selected(&mut candidates, candidate_index, "catalog".to_owned(), None);
            existing
        } else {
            let synced_cex_qty_step = synced_cex_qty_step(
                synced_cex_qty_steps,
                template.hedge_exchange,
                &template.cex_symbol,
                template.cex_qty_step,
            );
            match synthesize_market_config(&discovered_market, template, synced_cex_qty_step) {
                Ok(market) => {
                    summary.synthesized += 1;
                    mark_candidate_selected(
                        &mut candidates,
                        candidate_index,
                        "synthesized".to_owned(),
                        None,
                    );
                    market
                }
                Err(err) => {
                    let err_text = err.to_string();
                    summary.skipped_generation += 1;
                    warnings.push(format!(
                        "{}: skipped discovered market {} ({}) - {}",
                        asset.display_name,
                        discovered_market.market_slug,
                        discovered_market.market_id,
                        err_text
                    ));
                    mark_candidate_generation_failed(&mut candidates, candidate_index, err_text);
                    continue;
                }
            }
        };

        let original_symbol = market.symbol.0.clone();
        let (output_symbol, symbol_rewritten) =
            ensure_unique_market_symbol(&mut market, &discovered_market, &mut selected_symbols);
        if symbol_rewritten {
            warnings.push(format!(
                "{}: runtime symbol {} rewritten to {} for condition {}",
                asset.display_name, original_symbol, output_symbol, discovered_market.condition_id
            ));
        }
        mark_candidate_output_symbol(&mut candidates, candidate_index, output_symbol.clone());

        generated_markets.push(market);
        summary.selected += 1;
    }

    summary.warnings = asset_warning_count
        + summary.skipped_static_validation
        + summary.skipped_probe_validation
        + summary.skipped_generation
        + usize::from(selection.truncated > 0);

    Ok(RefreshAssetArtifact {
        summary: summary.clone(),
        report: RefreshAssetReport {
            asset: asset.display_name.to_owned(),
            discovered: summary.discovered,
            static_validated: summary.static_validated,
            validated: summary.validated,
            selection_target: selection.target_selected,
            selected: summary.selected,
            truncated: selection.truncated,
            catalog_match: summary.matched_catalog,
            synthesized: summary.synthesized,
            skipped_static_validation: summary.skipped_static_validation,
            skipped_probe_validation: summary.skipped_probe_validation,
            skipped_generation: summary.skipped_generation,
            discovery_warning_count: asset_warning_count,
            discovery_warning_samples,
            warnings: summary.warnings,
            rejected_reason_counts: collect_rejected_reason_counts(&candidates),
            lifecycle_status_counts: collect_lifecycle_status_counts(&candidates),
            candidates,
        },
        warnings,
        generated_markets,
    })
}

fn build_refresh_report(
    base_env: &str,
    catalog_env: &str,
    output_path: &str,
    validation_policy: MarketValidationPolicy,
    asset_reports: Vec<RefreshAssetReport>,
) -> RefreshActiveReport {
    let discovered = asset_reports.iter().map(|report| report.discovered).sum();
    let static_validated = asset_reports
        .iter()
        .map(|report| report.static_validated)
        .sum();
    let validated = asset_reports.iter().map(|report| report.validated).sum();
    let selected = asset_reports.iter().map(|report| report.selected).sum();
    let skipped_static_validation = asset_reports
        .iter()
        .map(|report| report.skipped_static_validation)
        .sum();
    let skipped_probe_validation = asset_reports
        .iter()
        .map(|report| report.skipped_probe_validation)
        .sum();
    let skipped_generation = asset_reports
        .iter()
        .map(|report| report.skipped_generation)
        .sum();

    let mut rejection_counts = BTreeMap::new();
    let mut lifecycle_counts = BTreeMap::new();
    for report in &asset_reports {
        for candidate in &report.candidates {
            if let Some(reason) = candidate.rejection_reason.as_deref() {
                *rejection_counts.entry(reason.to_owned()).or_insert(0usize) += 1;
            }
            *lifecycle_counts
                .entry(candidate.lifecycle_status.clone())
                .or_insert(0usize) += 1;
        }
    }

    RefreshActiveReport {
        report_version: 2,
        generated_at_utc: Utc::now().to_rfc3339(),
        base_env: base_env.to_owned(),
        catalog_env: catalog_env.to_owned(),
        output_toml: output_path.to_owned(),
        validation: RefreshValidationPolicyReport {
            max_markets_per_asset: validation_policy.max_markets_per_asset,
            min_minutes_to_settlement: validation_policy.min_minutes_to_settlement,
            min_liquidity_usd: validation_policy.min_liquidity_usd,
            min_volume_24h_usd: validation_policy.min_volume_24h_usd,
            clob_book_probe_enabled: true,
        },
        totals: RefreshReportTotals {
            discovered,
            static_validated,
            validated,
            selected,
            skipped_static_validation,
            skipped_probe_validation,
            skipped_generation,
            rejected_reason_counts: rejection_counts
                .into_iter()
                .filter_map(|(code, count)| {
                    validation_reason_from_code(&code).map(|reason| RefreshCountEntry {
                        code,
                        label_zh: reason.label_zh().to_owned(),
                        count,
                    })
                })
                .collect(),
            lifecycle_status_counts: lifecycle_counts
                .into_iter()
                .filter_map(|(code, count)| {
                    candidate_lifecycle_status_from_code(&code).map(|status| RefreshCountEntry {
                        code,
                        label_zh: status.label_zh().to_owned(),
                        count,
                    })
                })
                .collect(),
        },
        assets: asset_reports,
    }
}

fn derive_refresh_report_path(output_path: &Path) -> String {
    let file_stem = output_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("markets");
    let filename = format!("{file_stem}.report.json");

    match output_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            parent.join(filename).display().to_string()
        }
        _ => filename,
    }
}

async fn fetch_active_asset_markets(
    client: &Client,
    asset: AssetSpec,
    query: &str,
    limit: usize,
) -> Result<(Vec<DiscoveredMarket>, Vec<String>)> {
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    let mut seen_event_ids = HashSet::new();
    let mut seen_market_ids = HashSet::new();

    let mut queries = vec![query.to_owned()];
    if query == asset.default_query {
        queries.extend(
            asset
                .search_queries()
                .iter()
                .map(|keyword| keyword.to_string()),
        );
    }
    queries.sort();
    queries.dedup();

    for query in queries {
        for page in 1..=MAX_PUBLIC_SEARCH_PAGES {
            let payload = http_get_json(
                client,
                GAMMA_PUBLIC_SEARCH_URL,
                &[
                    ("q", query.clone()),
                    ("events_status", "active".to_owned()),
                    ("limit_per_type", limit.min(50).to_string()),
                    ("page", page.to_string()),
                    ("search_tags", "false".to_owned()),
                    ("search_profiles", "false".to_owned()),
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to fetch public-search query `{query}` page {}",
                    page
                )
            })?;

            let events = payload
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if events.is_empty() {
                break;
            }

            let event_ids = events
                .into_iter()
                .filter_map(|summary| {
                    let event_id = string_field(&summary, "id");
                    if event_id.is_empty() || !seen_event_ids.insert(event_id.clone()) {
                        None
                    } else {
                        Some(event_id)
                    }
                })
                .collect::<Vec<_>>();
            let mut event_fetches =
                futures_util::stream::iter(event_ids.into_iter().map(|event_id| {
                    let client = client.clone();
                    async move {
                        let full_event = fetch_event_by_id(&client, &event_id).await;
                        (event_id, full_event)
                    }
                }))
                .buffer_unordered(EVENT_FETCH_CONCURRENCY);

            while let Some((event_id, full_event)) = event_fetches.next().await {
                let full_event = full_event.with_context(|| {
                    format!(
                        "failed to fetch full event {} for query `{}` page {}",
                        event_id, query, page
                    )
                })?;
                let Some(full_event) = full_event else {
                    warnings.push(format!(
                        "{}: 跳过缺失详情的事件 {}（query=`{}` page={}）",
                        asset.display_name, event_id, query, page
                    ));
                    continue;
                };
                if !is_asset_event(&full_event, asset) {
                    continue;
                }
                for market in extract_markets_from_event(&full_event, &mut warnings) {
                    if seen_market_ids.insert(market.market_id.clone()) {
                        out.push(market);
                        if out.len() >= limit {
                            return Ok((out, warnings));
                        }
                    }
                }
            }

            let has_more = payload
                .get("pagination")
                .and_then(Value::as_object)
                .and_then(|pagination| pagination.get("hasMore"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !has_more {
                break;
            }
        }
    }

    Ok((out, warnings))
}

async fn fetch_event_by_id(client: &Client, event_id: &str) -> Result<Option<Value>> {
    let payload = http_get_json(client, GAMMA_EVENTS_URL, &[("id", event_id.to_owned())])
        .await
        .with_context(|| format!("failed to fetch event {}", event_id))?;

    parse_event_lookup_payload(payload, event_id)
}

fn parse_event_lookup_payload(payload: Value, event_id: &str) -> Result<Option<Value>> {
    let events = payload
        .as_array()
        .ok_or_else(|| anyhow!("gamma returned non-array event payload for id {}", event_id))?;
    Ok(events.first().cloned())
}

fn extract_markets_from_event(event: &Value, warnings: &mut Vec<String>) -> Vec<DiscoveredMarket> {
    let event_id = string_field(event, "id");
    let event_title = string_field(event, "title");
    let event_slug = string_field(event, "slug");
    let event_end = parse_dt(event.get("endDate")).or_else(|| parse_dt(event.get("endDateIso")));
    let event_active = bool_field(event, "active").unwrap_or(true);
    let event_closed = bool_field(event, "closed").unwrap_or(false);
    let event_archived = bool_field(event, "archived").unwrap_or(false);

    let Some(markets) = event.get("markets").and_then(Value::as_array) else {
        warnings.push(format!("event {event_id} has no market array"));
        return Vec::new();
    };

    let mut out = Vec::new();
    for market in markets {
        let market_id = string_field(market, "id");
        if market_id.is_empty() {
            warnings.push(format!("event {event_id} contains market with missing id"));
            continue;
        }

        let market_active = bool_field(market, "active").unwrap_or(event_active);
        let market_closed = bool_field(market, "closed").unwrap_or(event_closed);
        if !market_active || market_closed {
            continue;
        }
        let market_archived = bool_field(market, "archived").unwrap_or(event_archived);

        let condition_id = string_field(market, "conditionId");
        if condition_id.is_empty() {
            warnings.push(format!("market {market_id} skipped: conditionId missing"));
            continue;
        }

        let token_ids = parse_token_ids(market.get("clobTokenIds"));
        if token_ids.len() != 2 {
            warnings.push(format!(
                "market {market_id} skipped: expected 2 token ids, got {}",
                token_ids.len()
            ));
            continue;
        }

        let outcomes = parse_string_array(market.get("outcomes"));
        if !is_standard_yes_no_outcomes(&outcomes) {
            warnings.push(format!(
                "market {market_id} skipped: outcomes are not standard Yes/No -> {:?}",
                outcomes
            ));
            continue;
        }

        let settlement = first_non_none_dt([
            parse_dt(market.get("endDate")),
            parse_dt(event.get("endDate")),
            parse_dt(market.get("endDateIso")),
            event_end.clone(),
        ]);
        let Some(settlement) = settlement else {
            warnings.push(format!("market {market_id} skipped: endDate missing"));
            continue;
        };

        let market_slug = preferred_slug(market, &event_slug);
        let symbol = normalize_symbol(&market_slug);
        if symbol.is_empty() {
            warnings.push(format!("market {market_id} skipped: slug missing"));
            continue;
        }

        let question = preferred_question(market, &event_title);
        let market_rule = parse_market_rule(&question);
        let strike_price = market_rule
            .as_ref()
            .and_then(primary_strike_from_rule)
            .or_else(|| parse_strike_price(&question, &market_slug));
        out.push(DiscoveredMarket {
            event_id: event_id.clone(),
            event_title: event_title.clone(),
            event_slug: event_slug.clone(),
            market_id,
            question: question.clone(),
            market_slug: symbol,
            condition_id,
            yes_token_id: token_ids[0].clone(),
            no_token_id: token_ids[1].clone(),
            settlement_timestamp: settlement.timestamp() as u64,
            settlement_iso: settlement.to_rfc3339(),
            neg_risk: bool_field(market, "negRisk")
                .or_else(|| bool_field(event, "negRisk"))
                .unwrap_or(false),
            market_rule,
            strike_price,
            event_archived,
            market_archived,
            accepting_orders: bool_field(market, "acceptingOrders").unwrap_or(false),
            enable_orderbook: bool_field(market, "enableOrderBook").unwrap_or(false),
            best_bid: f64_field(market, "bestBid"),
            best_ask: f64_field(market, "bestAsk"),
            liquidity_num: f64_field(market, "liquidityNum"),
            volume_num: f64_field(market, "volumeNum"),
            volume_24h: f64_field(market, "volume24hr"),
        });
    }

    out
}

async fn http_get_json(client: &Client, url: &str, params: &[(&str, String)]) -> Result<Value> {
    let mut last_error = None;

    for attempt in 1..=HTTP_GET_RETRY_ATTEMPTS {
        let response = match client
            .get(url)
            .query(params)
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(anyhow!("failed to send GET {}: {}", url, err));
                if attempt < HTTP_GET_RETRY_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        HTTP_GET_RETRY_BACKOFF_MS * attempt as u64,
                    ))
                    .await;
                    continue;
                }
                break;
            }
        };

        let status = response.status();
        if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
            && attempt < HTTP_GET_RETRY_ATTEMPTS
        {
            tokio::time::sleep(std::time::Duration::from_millis(retry_backoff_ms(
                &response,
                attempt,
                HTTP_GET_RETRY_BACKOFF_MS,
            )))
            .await;
            continue;
        }

        let response = match response.error_for_status() {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(anyhow!("GET {} returned error status: {}", url, err));
                if attempt < HTTP_GET_RETRY_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        HTTP_GET_RETRY_BACKOFF_MS * attempt as u64,
                    ))
                    .await;
                    continue;
                }
                break;
            }
        };

        return response
            .json::<Value>()
            .await
            .with_context(|| format!("failed to decode JSON from {}", url));
    }

    Err(last_error.unwrap_or_else(|| anyhow!("GET {} failed after retries", url)))
}

async fn fetch_binance_cex_qty_steps(
    client: &Client,
    rest_url: &str,
    symbols: &[String],
) -> Result<CexQtyStepSyncMap> {
    let mut synced_steps = HashMap::new();
    let mut seen_symbols = HashSet::new();
    let url = format!(
        "{}/{}",
        rest_url.trim_end_matches('/'),
        BINANCE_EXCHANGE_INFO_PATH.trim_start_matches('/')
    );

    for symbol in symbols {
        let symbol = symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() || !seen_symbols.insert(symbol.clone()) {
            continue;
        }

        let payload = http_get_json(client, &url, &[("symbol", symbol.clone())])
            .await
            .with_context(|| format!("failed to fetch Binance exchangeInfo for {}", symbol))?;
        let step_size = parse_binance_exchange_info_step_size(&payload, &symbol)
            .with_context(|| format!("failed to parse Binance exchangeInfo for {}", symbol))?;
        synced_steps.insert((Exchange::Binance, symbol), step_size);
    }

    Ok(synced_steps)
}

fn parse_binance_exchange_info_step_size(payload: &Value, symbol: &str) -> Result<Decimal> {
    let symbols = payload
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Binance exchangeInfo missing symbols array for {}", symbol))?;
    let symbol_payload = symbols
        .iter()
        .find(|entry| string_field(entry, "symbol").eq_ignore_ascii_case(symbol))
        .ok_or_else(|| anyhow!("Binance exchangeInfo missing symbol {}", symbol))?;
    let filters = symbol_payload
        .get("filters")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Binance exchangeInfo missing filters for {}", symbol))?;
    let lot_size_filter = filters
        .iter()
        .find(|entry| string_field(entry, "filterType") == "LOT_SIZE")
        .ok_or_else(|| {
            anyhow!(
                "Binance exchangeInfo missing LOT_SIZE filter for {}",
                symbol
            )
        })?;
    let raw_step_size = lot_size_filter
        .get("stepSize")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Binance exchangeInfo missing LOT_SIZE.stepSize for {}",
                symbol
            )
        })?;

    Decimal::from_str(raw_step_size).with_context(|| {
        format!(
            "invalid Binance LOT_SIZE.stepSize `{}` for {}",
            raw_step_size, symbol
        )
    })
}

fn synced_cex_qty_step(
    synced_steps: &CexQtyStepSyncMap,
    exchange: Exchange,
    cex_symbol: &str,
    fallback: Decimal,
) -> Decimal {
    synced_steps
        .get(&(exchange, cex_symbol.trim().to_ascii_uppercase()))
        .copied()
        .unwrap_or(fallback)
}

fn apply_synced_cex_qty_step(
    mut market: MarketConfig,
    synced_steps: &CexQtyStepSyncMap,
) -> MarketConfig {
    market.cex_qty_step = synced_cex_qty_step(
        synced_steps,
        market.hedge_exchange,
        &market.cex_symbol,
        market.cex_qty_step,
    );
    market
}

fn validate_and_select_markets(
    discovered: Vec<DiscoveredMarket>,
    policy: MarketValidationPolicy,
) -> (
    ValidationSelection,
    Vec<(DiscoveredMarket, ValidationRejectionReason)>,
) {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();

    for market in discovered {
        match validate_discovered_market(&market, policy) {
            Ok(()) => accepted.push(market),
            Err(reason) => rejected.push((market, reason)),
        }
    }

    accepted.sort_by(compare_discovered_markets_for_runtime);
    let validated_total = accepted.len();
    let truncated = validated_total.saturating_sub(policy.max_markets_per_asset);

    (
        ValidationSelection {
            ranked_validated: accepted,
            validated_total,
            target_selected: validated_total.min(policy.max_markets_per_asset),
            truncated,
        },
        rejected,
    )
}

fn build_validation_selection(
    mut ranked_validated: Vec<DiscoveredMarket>,
    policy: MarketValidationPolicy,
) -> ValidationSelection {
    ranked_validated.sort_by(compare_discovered_markets_for_runtime);
    let validated_total = ranked_validated.len();
    let truncated = validated_total.saturating_sub(policy.max_markets_per_asset);
    ValidationSelection {
        ranked_validated,
        validated_total,
        target_selected: validated_total.min(policy.max_markets_per_asset),
        truncated,
    }
}

async fn probe_discovered_markets(
    client: &Client,
    clob_api_url: &str,
    ranked_markets: Vec<DiscoveredMarket>,
) -> Vec<(DiscoveredMarket, Option<ValidationRejectionReason>)> {
    let mut results = futures_util::stream::iter(ranked_markets.into_iter().enumerate().map(
        |(index, market)| {
            let client = client.clone();
            let clob_api_url = clob_api_url.to_owned();
            async move {
                let rejection = probe_single_market(&client, &clob_api_url, &market).await;
                (index, market, rejection)
            }
        },
    ))
    .buffer_unordered(CLOB_BOOK_PROBE_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    results.sort_by_key(|(index, _, _)| *index);
    results
        .into_iter()
        .map(|(_, market, rejection)| (market, rejection))
        .collect()
}

async fn probe_single_market(
    client: &Client,
    clob_api_url: &str,
    market: &DiscoveredMarket,
) -> Option<ValidationRejectionReason> {
    let yes_result = probe_single_token(client, clob_api_url, &market.yes_token_id).await;
    if yes_result.is_none() {
        return None;
    }

    let no_result = probe_single_token(client, clob_api_url, &market.no_token_id).await;
    if no_result.is_none() {
        return None;
    }

    if matches!(yes_result, Some(ValidationRejectionReason::ClobProbeFailed))
        || matches!(no_result, Some(ValidationRejectionReason::ClobProbeFailed))
    {
        Some(ValidationRejectionReason::ClobProbeFailed)
    } else {
        Some(ValidationRejectionReason::NoClobOrderbook)
    }
}

async fn probe_single_token(
    client: &Client,
    clob_api_url: &str,
    token_id: &str,
) -> Option<ValidationRejectionReason> {
    let endpoint = format!("{}/book", clob_api_url.trim_end_matches('/'));
    let mut last_reason = ValidationRejectionReason::ClobProbeFailed;

    for attempt in 1..=CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
        let response = match client
            .get(&endpoint)
            .query(&[("token_id", token_id)])
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                if attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        CLOB_BOOK_PROBE_RETRY_BACKOFF_MS * attempt as u64,
                    ))
                    .await;
                    continue;
                }
                return Some(last_reason);
            }
        };

        let status = response.status();
        if (status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
            && attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS
        {
            tokio::time::sleep(std::time::Duration::from_millis(retry_backoff_ms(
                &response,
                attempt,
                CLOB_BOOK_PROBE_RETRY_BACKOFF_MS,
            )))
            .await;
            continue;
        }

        let body = match response.text().await {
            Ok(body) => body,
            Err(_) => {
                if attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        CLOB_BOOK_PROBE_RETRY_BACKOFF_MS * attempt as u64,
                    ))
                    .await;
                    continue;
                }
                return Some(last_reason);
            }
        };

        if status.as_u16() == 404 || body_contains_no_orderbook(&body) {
            return Some(ValidationRejectionReason::NoClobOrderbook);
        }

        if !status.is_success() {
            if attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
                tokio::time::sleep(std::time::Duration::from_millis(
                    CLOB_BOOK_PROBE_RETRY_BACKOFF_MS * attempt as u64,
                ))
                .await;
                continue;
            }
            return Some(last_reason);
        }

        let payload = match serde_json::from_str::<Value>(&body) {
            Ok(payload) => payload,
            Err(_) => {
                if attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        CLOB_BOOK_PROBE_RETRY_BACKOFF_MS * attempt as u64,
                    ))
                    .await;
                    continue;
                }
                return Some(last_reason);
            }
        };

        if payload_indicates_no_orderbook(&payload) {
            return Some(ValidationRejectionReason::NoClobOrderbook);
        }

        let root = payload.get("book").unwrap_or(&payload);
        if payload_has_nonempty_orderbook(root) {
            return None;
        }
        if root.get("bids").is_some() || root.get("asks").is_some() {
            return Some(ValidationRejectionReason::NoClobOrderbook);
        }

        if attempt < CLOB_BOOK_PROBE_RETRY_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(
                CLOB_BOOK_PROBE_RETRY_BACKOFF_MS * attempt as u64,
            ))
            .await;
            last_reason = ValidationRejectionReason::ClobProbeFailed;
            continue;
        }
        return Some(last_reason);
    }

    Some(last_reason)
}

fn body_contains_no_orderbook(body: &str) -> bool {
    body.to_lowercase().contains("no orderbook exists")
}

fn payload_indicates_no_orderbook(payload: &Value) -> bool {
    let candidates = [
        payload.get("error"),
        payload.get("message"),
        payload.get("detail"),
        payload.get("msg"),
    ];
    candidates.into_iter().flatten().any(|value| {
        value
            .as_str()
            .map(body_contains_no_orderbook)
            .unwrap_or(false)
    })
}

fn payload_has_nonempty_orderbook(payload: &Value) -> bool {
    fn has_levels(value: Option<&Value>) -> bool {
        value
            .and_then(Value::as_array)
            .map(|levels| !levels.is_empty())
            .unwrap_or(false)
    }

    has_levels(payload.get("bids")) || has_levels(payload.get("asks"))
}

fn retry_backoff_ms(response: &reqwest::Response, attempt: usize, default_backoff_ms: u64) -> u64 {
    if let Some(retry_after_ms) = retry_after_header_ms(response) {
        return retry_after_ms.max(default_backoff_ms);
    }

    let multiplier = if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        5
    } else {
        1
    };
    default_backoff_ms * attempt as u64 * multiplier
}

fn retry_after_header_ms(response: &reqwest::Response) -> Option<u64> {
    let raw = response.headers().get(reqwest::header::RETRY_AFTER)?;
    let text = raw.to_str().ok()?.trim();
    let seconds = text.parse::<u64>().ok()?;
    Some(seconds.saturating_mul(1000))
}

fn validate_discovered_market(
    market: &DiscoveredMarket,
    policy: MarketValidationPolicy,
) -> std::result::Result<(), ValidationRejectionReason> {
    let now_ts = Utc::now().timestamp();
    let min_settlement_ts = now_ts + policy.min_minutes_to_settlement.saturating_mul(60);
    let has_activity_signal = market_has_activity_signal(market, policy);

    if market.event_archived {
        return Err(ValidationRejectionReason::EventArchived);
    }
    if market.market_archived {
        return Err(ValidationRejectionReason::MarketArchived);
    }
    if market.settlement_timestamp as i64 <= now_ts {
        return Err(ValidationRejectionReason::Settled);
    }
    if market.settlement_timestamp as i64 <= min_settlement_ts {
        return Err(ValidationRejectionReason::SettlesTooSoon);
    }
    if let Some(reason) = main_strategy_rejection_reason(
        &policy.market_scope,
        Some(&market.question),
        &market.market_slug,
        market.market_rule.as_ref(),
    ) {
        return Err(match reason {
            "path_dependent_market_not_supported" => {
                ValidationRejectionReason::PathDependentMarketNotSupported
            }
            "all_time_high_market_not_supported" => {
                ValidationRejectionReason::AllTimeHighMarketNotSupported
            }
            "terminal_price_market_not_supported" => {
                ValidationRejectionReason::TerminalPriceMarketNotSupported
            }
            _ => ValidationRejectionReason::NonPriceMarketNotSupported,
        });
    }
    if !market.accepting_orders {
        return Err(ValidationRejectionReason::NotAcceptingOrders);
    }
    if !market.enable_orderbook {
        return Err(ValidationRejectionReason::OrderBookDisabled);
    }
    if !has_activity_signal {
        return Err(ValidationRejectionReason::MissingTradabilitySignal);
    }
    Ok(())
}

fn market_has_live_bbo(market: &DiscoveredMarket) -> bool {
    let best_bid = market.best_bid.unwrap_or_default();
    let best_ask = market.best_ask.unwrap_or_default();
    best_bid > 0.0 && best_ask > 0.0 && best_bid < best_ask && best_bid < 1.0 && best_ask <= 1.0
}

fn market_has_activity_signal(market: &DiscoveredMarket, policy: MarketValidationPolicy) -> bool {
    market_has_live_bbo(market)
        || market.liquidity_num.unwrap_or_default() >= policy.min_liquidity_usd
        || market.volume_24h.unwrap_or_default() >= policy.min_volume_24h_usd
}

fn compare_discovered_markets_for_runtime(
    left: &DiscoveredMarket,
    right: &DiscoveredMarket,
) -> std::cmp::Ordering {
    let left_has_bbo = market_has_live_bbo(left);
    let right_has_bbo = market_has_live_bbo(right);

    right_has_bbo
        .cmp(&left_has_bbo)
        .then_with(|| {
            right
                .volume_24h
                .unwrap_or_default()
                .partial_cmp(&left.volume_24h.unwrap_or_default())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            right
                .liquidity_num
                .unwrap_or_default()
                .partial_cmp(&left.liquidity_num.unwrap_or_default())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            right
                .volume_num
                .unwrap_or_default()
                .partial_cmp(&left.volume_num.unwrap_or_default())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.settlement_timestamp.cmp(&right.settlement_timestamp))
        .then_with(|| left.market_slug.cmp(&right.market_slug))
}

fn print_candidates(candidates: &[DiscoveredMarket], asset: AssetSpec) {
    println!("discovered active {} markets:", asset.display_name);
    for (idx, market) in candidates.iter().enumerate() {
        println!(
            "[{}] symbol={} settle={} strike={} liq={} vol24h={} bbo={}/{} market_id={} event_id={} question={}",
            idx,
            market.market_slug,
            market.settlement_iso,
            render_optional_price(&market.strike_price),
            render_optional_number(market.liquidity_num),
            render_optional_number(market.volume_24h),
            render_optional_number(market.best_bid),
            render_optional_number(market.best_ask),
            market.market_id,
            market.event_id,
            market.question
        );
    }
}

fn print_discovery_warnings(warnings: &[String]) {
    if warnings.is_empty() {
        return;
    }

    println!("discovery warnings:");
    for warning in warnings.iter().take(20) {
        println!("  - {warning}");
    }
    if warnings.len() > 20 {
        println!("  - ... {} more", warnings.len() - 20);
    }
}

fn build_candidate_report(
    market: &DiscoveredMarket,
    validation_policy: MarketValidationPolicy,
    rank: Option<usize>,
    probe_checked: bool,
    lifecycle_status: CandidateLifecycleStatus,
    rejection_reason: Option<ValidationRejectionReason>,
    generation_error: Option<String>,
    catalog_available: bool,
    runtime_source: Option<String>,
) -> RefreshCandidateReport {
    let has_live_bbo = market_has_live_bbo(market);
    let has_activity_signal = market_has_activity_signal(market, validation_policy);

    RefreshCandidateReport {
        market_slug: market.market_slug.clone(),
        market_id: market.market_id.clone(),
        condition_id: market.condition_id.clone(),
        event_id: market.event_id.clone(),
        event_title: market.event_title.clone(),
        event_slug: market.event_slug.clone(),
        question: market.question.clone(),
        settlement_utc: market.settlement_iso.clone(),
        settlement_timestamp: market.settlement_timestamp,
        rank,
        probe_checked,
        lifecycle_status: lifecycle_status.code().to_owned(),
        lifecycle_status_zh: lifecycle_status.label_zh().to_owned(),
        output_symbol: None,
        rejection_reason: rejection_reason.map(|reason| reason.code().to_owned()),
        rejection_reason_zh: rejection_reason.map(|reason| reason.label_zh().to_owned()),
        generation_error,
        runtime_selected: lifecycle_status == CandidateLifecycleStatus::Selected,
        runtime_source,
        catalog_available,
        accepting_orders: market.accepting_orders,
        orderbook_enabled: market.enable_orderbook,
        event_archived: market.event_archived,
        market_archived: market.market_archived,
        has_live_bbo,
        has_activity_signal,
        best_bid: market.best_bid,
        best_ask: market.best_ask,
        liquidity_usd: market.liquidity_num,
        volume_total_usd: market.volume_num,
        volume_24h_usd: market.volume_24h,
    }
}

fn mark_candidate_selected(
    candidates: &mut [RefreshCandidateReport],
    index: Option<usize>,
    runtime_source: String,
    generation_error: Option<String>,
) {
    let Some(index) = index else {
        return;
    };
    let Some(candidate) = candidates.get_mut(index) else {
        return;
    };
    candidate.lifecycle_status = CandidateLifecycleStatus::Selected.code().to_owned();
    candidate.lifecycle_status_zh = CandidateLifecycleStatus::Selected.label_zh().to_owned();
    candidate.runtime_selected = true;
    candidate.runtime_source = Some(runtime_source);
    candidate.generation_error = generation_error;
}

fn mark_candidate_generation_failed(
    candidates: &mut [RefreshCandidateReport],
    index: Option<usize>,
    generation_error: String,
) {
    let Some(index) = index else {
        return;
    };
    let Some(candidate) = candidates.get_mut(index) else {
        return;
    };
    candidate.lifecycle_status = CandidateLifecycleStatus::GenerationFailed.code().to_owned();
    candidate.lifecycle_status_zh = CandidateLifecycleStatus::GenerationFailed
        .label_zh()
        .to_owned();
    candidate.runtime_selected = false;
    candidate.runtime_source = None;
    candidate.generation_error = Some(generation_error);
}

fn mark_candidate_output_symbol(
    candidates: &mut [RefreshCandidateReport],
    index: Option<usize>,
    output_symbol: String,
) {
    let Some(index) = index else {
        return;
    };
    let Some(candidate) = candidates.get_mut(index) else {
        return;
    };
    candidate.output_symbol = Some(output_symbol);
}

fn collect_rejected_reason_counts(candidates: &[RefreshCandidateReport]) -> Vec<RefreshCountEntry> {
    let mut counts = BTreeMap::new();
    for candidate in candidates {
        if let Some(reason) = candidate.rejection_reason.as_deref() {
            *counts.entry(reason.to_owned()).or_insert(0usize) += 1;
        }
    }

    counts
        .into_iter()
        .filter_map(|(code, count)| {
            validation_reason_from_code(&code).map(|reason| RefreshCountEntry {
                code,
                label_zh: reason.label_zh().to_owned(),
                count,
            })
        })
        .collect()
}

fn collect_lifecycle_status_counts(
    candidates: &[RefreshCandidateReport],
) -> Vec<RefreshCountEntry> {
    let mut counts = BTreeMap::new();
    for candidate in candidates {
        *counts
            .entry(candidate.lifecycle_status.clone())
            .or_insert(0usize) += 1;
    }

    counts
        .into_iter()
        .filter_map(|(code, count)| {
            candidate_lifecycle_status_from_code(&code).map(|status| RefreshCountEntry {
                code,
                label_zh: status.label_zh().to_owned(),
                count,
            })
        })
        .collect()
}

fn ensure_unique_market_symbol(
    market: &mut MarketConfig,
    discovered: &DiscoveredMarket,
    used_symbols: &mut HashSet<String>,
) -> (String, bool) {
    let original_symbol = market.symbol.0.clone();
    for candidate in [original_symbol.clone(), discovered.market_slug.clone()] {
        if candidate.is_empty() {
            continue;
        }
        if used_symbols.insert(candidate.clone()) {
            if candidate != market.symbol.0 {
                market.symbol = Symbol::new(candidate.clone());
            }
            return (candidate.clone(), candidate != original_symbol);
        }
    }

    let base = if discovered.market_slug.is_empty() {
        original_symbol.clone()
    } else {
        discovered.market_slug.clone()
    };
    let unique = uniquify_symbol(&base, &market.poly_ids.condition_id, used_symbols);
    market.symbol = Symbol::new(unique.clone());
    (unique.clone(), unique != original_symbol)
}

fn uniquify_symbol(base: &str, condition_id: &str, used_symbols: &mut HashSet<String>) -> String {
    let suffix = short_condition_suffix(condition_id);
    let primary = format!("{base}-{suffix}");
    if used_symbols.insert(primary.clone()) {
        return primary;
    }

    let mut counter = 2usize;
    loop {
        let candidate = format!("{base}-{suffix}-{counter}");
        if used_symbols.insert(candidate.clone()) {
            return candidate;
        }
        counter += 1;
    }
}

fn short_condition_suffix(condition_id: &str) -> String {
    let sanitized = condition_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_lowercase();
    let suffix_len = sanitized.len().min(8);
    sanitized[sanitized.len().saturating_sub(suffix_len)..].to_owned()
}

fn write_output(path: &str, payload: &str) -> Result<()> {
    let output_path = Path::new(path);
    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory `{}`", parent.display())
            })?;
        }
    }
    fs::write(output_path, payload).with_context(|| {
        format!(
            "failed to write discovery output `{}`",
            output_path.display()
        )
    })?;

    println!("discovery output written: {}", output_path.display());
    if let Some(env_name) = derive_env_name(output_path) {
        println!(
            "use it with: polyalpha-cli live-data-check --env {} --market-index 0",
            env_name
        );
    }
    Ok(())
}

fn derive_env_name(path: &Path) -> Option<String> {
    if path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        != Some("config")
    {
        return None;
    }
    if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
        return None;
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_owned)
}

fn render_market_snippet(
    discovered: &DiscoveredMarket,
    template: &MarketConfig,
    asset: AssetSpec,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# Auto-generated by `polyalpha-cli markets discover-{}`\n",
        asset.command_suffix()
    ));
    out.push_str(&format!("# source_event_id = {}\n", discovered.event_id));
    out.push_str(&format!(
        "# source_event_slug = {}\n",
        discovered.event_slug
    ));
    out.push_str(&format!(
        "# source_title = {}\n",
        toml_comment_text(&discovered.event_title)
    ));
    out.push_str(&format!(
        "# source_question = {}\n",
        toml_comment_text(&discovered.question)
    ));
    out.push_str(&format!("# source_market_id = {}\n", discovered.market_id));
    out.push_str(&format!(
        "# settlement_utc = {}\n",
        discovered.settlement_iso
    ));
    out.push_str("\n[[markets]]\n");
    out.push_str(&format!("symbol = \"{}\"\n", discovered.market_slug));
    out.push_str(&format!("condition_id = \"{}\"\n", discovered.condition_id));
    out.push_str(&format!("yes_token_id = \"{}\"\n", discovered.yes_token_id));
    out.push_str(&format!("no_token_id = \"{}\"\n", discovered.no_token_id));
    out.push_str(&format!(
        "market_question = {}\n",
        toml_string_text(&discovered.question)
    ));
    if let Some(rule) = &discovered.market_rule {
        out.push_str(&format!("market_rule = {}\n", render_market_rule(rule)));
    }
    out.push_str(&format!(
        "settlement_timestamp = {}\n",
        discovered.settlement_timestamp
    ));
    out.push_str(&format!(
        "min_tick_size = {}\n",
        template.min_tick_size.0.normalize()
    ));
    out.push_str(&format!("neg_risk = {}\n", discovered.neg_risk));
    out.push_str(&format!("cex_symbol = \"{}\"\n", template.cex_symbol));
    out.push_str(&format!(
        "hedge_exchange = \"{:?}\"\n",
        template.hedge_exchange
    ));
    if let Some(strike) = &discovered.strike_price {
        out.push_str(&format!("strike_price = {}\n", strike.0.normalize()));
    }
    out.push_str(&format!(
        "cex_price_tick = {}\n",
        template.cex_price_tick.normalize()
    ));
    out.push_str(&format!(
        "cex_qty_step = {}\n",
        template.cex_qty_step.normalize()
    ));
    out.push_str(&format!(
        "cex_contract_multiplier = {}\n",
        template.cex_contract_multiplier.normalize()
    ));
    out
}

fn render_generated_settings(
    settings: &Settings,
    base_env: &str,
    catalog_env: &str,
    assets: &[AssetSpec],
    summaries: &[RefreshAssetSummary],
    validation_policy: MarketValidationPolicy,
) -> Result<String> {
    let mut out = String::new();
    out.push_str("# Auto-generated by `polyalpha-cli markets refresh-active`\n");
    out.push_str(&format!("# base_env = {base_env}\n"));
    out.push_str(&format!("# catalog_env = {catalog_env}\n"));
    out.push_str(&format!(
        "# generated_at_utc = {}\n",
        Utc::now().to_rfc3339()
    ));
    out.push_str(&format!(
        "# assets = {}\n",
        assets
            .iter()
            .map(|asset| asset.display_name)
            .collect::<Vec<_>>()
            .join(",")
    ));
    out.push_str(&format!(
        "# validation = max_per_asset:{} min_minutes_to_settlement:{} min_liquidity_usd:{:.0} min_volume_24h_usd:{:.0} clob_book_probe:true\n",
        validation_policy.max_markets_per_asset,
        validation_policy.min_minutes_to_settlement,
        validation_policy.min_liquidity_usd,
        validation_policy.min_volume_24h_usd,
    ));
    for summary in summaries {
        out.push_str(&format!(
            "# {} discovered={} static_validated={} probe_validated={} selected={} catalog_match={} synthesized={} skipped_static_validation={} skipped_probe_validation={} skipped_generation={} warnings={}\n",
            summary.display_name,
            summary.discovered,
            summary.static_validated,
            summary.validated,
            summary.selected,
            summary.matched_catalog,
            summary.synthesized,
            summary.skipped_static_validation,
            summary.skipped_probe_validation,
            summary.skipped_generation,
            summary.warnings
        ));
    }
    out.push('\n');
    out.push_str(
        &toml::to_string_pretty(settings)
            .context("failed to serialize generated settings to TOML")?,
    );
    Ok(out)
}

fn market_matches(market: &DiscoveredMarket, needle: &str) -> bool {
    let needle = needle.trim().to_lowercase();
    if needle.is_empty() {
        return true;
    }

    let fields = [
        market.market_slug.as_str(),
        market.question.as_str(),
        market.event_title.as_str(),
        market.event_slug.as_str(),
    ];
    fields
        .iter()
        .any(|field| field.to_lowercase().contains(&needle))
}

fn preferred_slug(market: &Value, event_slug: &str) -> String {
    let market_slug = string_field(market, "slug");
    if !market_slug.is_empty() {
        market_slug
    } else {
        event_slug.to_owned()
    }
}

fn preferred_question(market: &Value, event_title: &str) -> String {
    let question = string_field(market, "question");
    if !question.is_empty() {
        question
    } else {
        event_title.to_owned()
    }
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .map(value_to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        _ => String::new(),
    }
}

fn bool_field(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn f64_field(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|value| match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    })
}

fn parse_token_ids(raw: Option<&Value>) -> Vec<String> {
    parse_string_array(raw)
}

fn parse_string_array(raw: Option<&Value>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };

    match raw {
        Value::Array(items) => items
            .iter()
            .map(value_to_string)
            .filter(|item| !item.trim().is_empty())
            .collect(),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Vec::new();
            }

            if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
                if let Value::Array(items) = parsed {
                    return items
                        .iter()
                        .map(value_to_string)
                        .filter(|item| !item.trim().is_empty())
                        .collect();
                }
                let single = value_to_string(&parsed);
                if single.trim().is_empty() {
                    Vec::new()
                } else {
                    vec![single]
                }
            } else {
                vec![trimmed.to_owned()]
            }
        }
        _ => {
            let single = value_to_string(raw);
            if single.trim().is_empty() {
                Vec::new()
            } else {
                vec![single]
            }
        }
    }
}

fn parse_dt(raw: Option<&Value>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    let text = value_to_string(raw);
    parse_dt_str(&text)
}

fn parse_dt_str(raw: &str) -> Option<DateTime<Utc>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc));
    }

    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return date.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc());
    }

    if trimmed.contains(' ') && !trimmed.contains('T') {
        let normalized = trimmed.replacen(' ', "T", 1);
        if let Ok(dt) = DateTime::parse_from_rfc3339(&normalized) {
            return Some(dt.with_timezone(&Utc));
        }
    }

    None
}

fn first_non_none_dt<const N: usize>(items: [Option<DateTime<Utc>>; N]) -> Option<DateTime<Utc>> {
    items.into_iter().flatten().next()
}

fn is_standard_yes_no_outcomes(outcomes: &[String]) -> bool {
    if outcomes.len() != 2 {
        return false;
    }
    normalize_binary_outcome(&outcomes[0]) == "yes"
        && normalize_binary_outcome(&outcomes[1]) == "no"
}

fn normalize_binary_outcome(value: &str) -> String {
    value.trim().to_lowercase()
}

fn normalize_symbol(slug: &str) -> String {
    slug.trim().trim_matches('/').to_lowercase()
}

fn parse_strike_price(question: &str, slug: &str) -> Option<Price> {
    if !looks_like_any_price_market_text(question, slug) {
        return None;
    }
    parse_market_rule(question)
        .as_ref()
        .and_then(primary_strike_from_rule)
        .or_else(|| parse_price_hint(question))
        .or_else(|| parse_price_hint(slug))
}

fn parse_market_rule(question: &str) -> Option<MarketRule> {
    parse_supported_terminal_market_rule(question).or_else(|| {
        let normalized = question
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();

        let above = Regex::new(&format!(
            r"(reach(?:es)?|hit(?:s)?|touch(?:es)?)\s+\$?{money}",
            money = MONEY_CAPTURE_RE
        ))
        .ok()?;
        if let Some(captures) = above.captures(&normalized) {
            return Some(MarketRule {
                kind: MarketRuleKind::Above,
                lower_strike: Some(Price(parse_money_capture(captures.get(2)?.as_str())?)),
                upper_strike: None,
            });
        }

        let below = Regex::new(&format!(
            r"(dip to|fall to|falls to|drop to|drops to)\s+\$?{money}",
            money = MONEY_CAPTURE_RE
        ))
        .ok()?;
        if let Some(captures) = below.captures(&normalized) {
            return Some(MarketRule {
                kind: MarketRuleKind::Below,
                lower_strike: None,
                upper_strike: Some(Price(parse_money_capture(captures.get(2)?.as_str())?)),
            });
        }

        None
    })
}

fn parse_money_capture(raw: &str) -> Option<Decimal> {
    let normalized = raw.replace(',', "").trim().to_lowercase();
    if normalized.is_empty() {
        return None;
    }

    let (number_text, multiplier) = match normalized.chars().last() {
        Some('k') => (&normalized[..normalized.len() - 1], Decimal::from(1_000u32)),
        Some('m') => (
            &normalized[..normalized.len() - 1],
            Decimal::from(1_000_000u32),
        ),
        Some('b') => (
            &normalized[..normalized.len() - 1],
            Decimal::from(1_000_000_000u64),
        ),
        Some(ch) if ch.is_ascii_alphabetic() => return None,
        Some(_) => (normalized.as_str(), Decimal::ONE),
        None => return None,
    };

    Decimal::from_str(number_text.trim())
        .ok()
        .map(|value| value * multiplier)
}

fn parse_price_hint(text: &str) -> Option<Price> {
    let lower = text
        .to_lowercase()
        .replace(',', " ")
        .replace('$', " ")
        .replace('?', " ");
    for token in lower
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '.')
        .filter(|token| !token.is_empty())
    {
        if let Some(value) = parse_numeric_token(token) {
            if is_probable_year(value) {
                continue;
            }
            return Some(Price(value));
        }
    }
    None
}

fn parse_numeric_token(token: &str) -> Option<Decimal> {
    if token.is_empty() {
        return None;
    }
    parse_money_capture(token)
}

fn is_probable_year(value: Decimal) -> bool {
    value >= Decimal::from(1900u32) && value <= Decimal::from(2100u32)
}

fn render_optional_price(price: &Option<Price>) -> String {
    price
        .as_ref()
        .map(|price| price.0.normalize().to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn render_optional_number(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.0}"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn render_market_rule(rule: &MarketRule) -> String {
    let kind = match rule.kind {
        MarketRuleKind::Above => "above",
        MarketRuleKind::Below => "below",
        MarketRuleKind::Between => "between",
    };
    let mut fields = vec![format!("kind = \"{kind}\"")];
    if let Some(lower) = rule.lower_strike {
        fields.push(format!("lower_strike = {}", lower.0.normalize()));
    }
    if let Some(upper) = rule.upper_strike {
        fields.push(format!("upper_strike = {}", upper.0.normalize()));
    }
    format!("{{ {} }}", fields.join(", "))
}

fn toml_comment_text(text: &str) -> String {
    text.replace('\n', " ").replace('\r', " ")
}

fn toml_string_text(text: &str) -> String {
    serde_json::to_string(&toml_comment_text(text)).unwrap_or_else(|_| "\"\"".to_owned())
}

fn is_asset_event(event: &Value, asset: AssetSpec) -> bool {
    let mut fields = vec![
        string_field(event, "title"),
        string_field(event, "slug"),
        string_field(event, "ticker"),
        string_field(event, "description"),
    ];

    if let Some(markets) = event.get("markets").and_then(Value::as_array) {
        for market in markets {
            fields.push(string_field(market, "question"));
            fields.push(string_field(market, "slug"));
            fields.push(string_field(market, "description"));
        }
    }

    asset.keywords.iter().any(|keyword| {
        fields
            .iter()
            .any(|field| field_contains_keyword(field, keyword))
    })
}

fn field_contains_keyword(field: &str, keyword: &str) -> bool {
    let field = field.to_lowercase();
    let keyword = keyword.to_lowercase();
    field
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .any(|token| token == keyword)
}

fn select_template_market(
    settings: &Settings,
    asset: AssetSpec,
    template_market_index: Option<usize>,
) -> Result<MarketConfig> {
    if let Some(index) = template_market_index {
        return select_market(settings, index);
    }

    find_asset_template_market(settings, asset).ok_or_else(|| {
        anyhow!(
            "no template market found for {} in env; rerun with --template-market-index if needed",
            asset.display_name
        )
    })
}

fn find_asset_template_market(settings: &Settings, asset: AssetSpec) -> Option<MarketConfig> {
    settings
        .markets
        .iter()
        .find(|market| market.cex_symbol.eq_ignore_ascii_case(asset.cex_symbol))
        .cloned()
}

fn resolve_assets(assets: &[MarketAsset]) -> Vec<AssetSpec> {
    let requested = if assets.is_empty() {
        vec![
            MarketAsset::Btc,
            MarketAsset::Eth,
            MarketAsset::Sol,
            MarketAsset::Xrp,
        ]
    } else {
        assets.to_vec()
    };
    let mut seen = HashSet::new();
    let mut resolved = Vec::new();
    for asset in requested {
        if seen.insert(asset) {
            resolved.push(AssetSpec::from_asset(asset));
        }
    }
    resolved
}

fn synthesize_market_config(
    discovered: &DiscoveredMarket,
    template: &MarketConfig,
    cex_qty_step: Decimal,
) -> Result<MarketConfig> {
    let market_rule = discovered.market_rule.clone();
    let strike_price = discovered
        .strike_price
        .or_else(|| market_rule.as_ref().and_then(primary_strike_from_rule))
        .ok_or_else(|| anyhow!("unable to determine strike_price"))?;

    Ok(MarketConfig {
        symbol: Symbol::new(discovered.market_slug.clone()),
        poly_ids: template.poly_ids.clone().with_condition(
            &discovered.condition_id,
            &discovered.yes_token_id,
            &discovered.no_token_id,
        ),
        market_question: Some(discovered.question.clone()),
        market_rule,
        cex_symbol: template.cex_symbol.clone(),
        hedge_exchange: template.hedge_exchange,
        strike_price: Some(strike_price),
        settlement_timestamp: discovered.settlement_timestamp,
        min_tick_size: template.min_tick_size,
        neg_risk: discovered.neg_risk,
        cex_price_tick: template.cex_price_tick,
        cex_qty_step,
        cex_contract_multiplier: template.cex_contract_multiplier,
    })
}

fn refresh_catalog_market_config(
    mut existing: MarketConfig,
    discovered: &DiscoveredMarket,
) -> MarketConfig {
    existing.symbol = Symbol::new(discovered.market_slug.clone());
    existing.poly_ids = existing.poly_ids.clone().with_condition(
        &discovered.condition_id,
        &discovered.yes_token_id,
        &discovered.no_token_id,
    );
    existing.market_question = Some(discovered.question.clone());
    existing.market_rule = discovered
        .market_rule
        .clone()
        .or(existing.market_rule.clone());
    existing.strike_price = discovered.strike_price.or(existing.strike_price);
    existing.settlement_timestamp = discovered.settlement_timestamp;
    existing.neg_risk = discovered.neg_risk;
    existing
}

fn compare_markets_for_output(left: &MarketConfig, right: &MarketConfig) -> std::cmp::Ordering {
    market_asset_rank(&left.cex_symbol)
        .cmp(&market_asset_rank(&right.cex_symbol))
        .then(left.settlement_timestamp.cmp(&right.settlement_timestamp))
        .then(
            left.strike_price
                .unwrap_or_default()
                .cmp(&right.strike_price.unwrap_or_default()),
        )
        .then(left.symbol.0.cmp(&right.symbol.0))
}

fn market_asset_rank(symbol: &str) -> usize {
    match symbol {
        "BTCUSDT" => 0,
        "ETHUSDT" => 1,
        "SOLUSDT" => 2,
        "XRPUSDT" => 3,
        _ => 100,
    }
}

fn primary_strike_from_rule(rule: &MarketRule) -> Option<Price> {
    match rule.kind {
        MarketRuleKind::Above => rule.lower_strike,
        MarketRuleKind::Below => rule.upper_strike,
        MarketRuleKind::Between => rule.lower_strike.or(rule.upper_strike),
    }
}

trait ReplacePolyIds {
    fn with_condition(self, condition_id: &str, yes_token_id: &str, no_token_id: &str) -> Self;
}

impl ReplacePolyIds for polyalpha_core::PolymarketIds {
    fn with_condition(self, condition_id: &str, yes_token_id: &str, no_token_id: &str) -> Self {
        Self {
            condition_id: condition_id.to_owned(),
            yes_token_id: yes_token_id.to_owned(),
            no_token_id: no_token_id.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_core::{
        AuditConfig, BinanceConfig, Exchange, GeneralConfig, MarketDataConfig,
        NegRiskStrategyConfig, OkxConfig, PaperSlippageConfig, PaperTradingConfig,
        PolymarketConfig, PolymarketIds, RiskConfig, Settings, StrategyConfig, UsdNotional,
    };
    use rust_decimal::Decimal;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn spawn_static_http_server(status_line: &str, body: &str, max_requests: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        thread::spawn(move || {
            for _ in 0..max_requests {
                let (mut stream, _) = listener.accept().expect("accept test connection");
                let mut buffer = [0_u8; 1024];
                let _ = stream.read(&mut buffer);
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write test response");
            }
        });
        format!("http://{}", addr)
    }

    #[test]
    fn parse_token_ids_handles_json_string_array() {
        let raw = Value::String("[\"yes-token\",\"no-token\"]".to_owned());
        assert_eq!(
            parse_token_ids(Some(&raw)),
            vec!["yes-token".to_owned(), "no-token".to_owned()]
        );
    }

    #[test]
    fn outcome_validation_requires_yes_then_no() {
        assert!(is_standard_yes_no_outcomes(&[
            "Yes".to_owned(),
            "No".to_owned()
        ]));
        assert!(!is_standard_yes_no_outcomes(&[
            "No".to_owned(),
            "Yes".to_owned()
        ]));
    }

    #[test]
    fn parse_dt_supports_rfc3339_and_date_only() {
        assert!(parse_dt_str("2026-03-31T04:00:00Z").is_some());
        assert!(parse_dt_str("2026-03-31").is_some());
    }

    #[test]
    fn parse_price_hint_understands_k_suffix() {
        let price = parse_strike_price(
            "Will Bitcoin reach 100k in March 2026?",
            "will-bitcoin-reach-100k-in-march-2026",
        )
        .expect("price should parse");
        assert_eq!(price.0.normalize().to_string(), "100000");
    }

    #[test]
    fn parse_market_rule_supports_between_questions() {
        let rule = parse_market_rule("Will Bitcoin be between $90,000 and $95,000 on Friday?")
            .expect("rule should parse");
        assert_eq!(rule.kind, MarketRuleKind::Between);
        assert_eq!(
            rule.lower_strike
                .expect("lower strike")
                .0
                .normalize()
                .to_string(),
            "90000"
        );
        assert_eq!(
            rule.upper_strike
                .expect("upper strike")
                .0
                .normalize()
                .to_string(),
            "95000"
        );
    }

    #[test]
    fn parse_market_rule_supports_reach_and_dip_questions() {
        let reach = parse_market_rule("Will Ethereum reach $3,000 in March?")
            .expect("reach rule should parse");
        assert_eq!(reach.kind, MarketRuleKind::Above);
        assert_eq!(
            reach
                .lower_strike
                .expect("lower strike")
                .0
                .normalize()
                .to_string(),
            "3000"
        );

        let dip =
            parse_market_rule("Will XRP dip to $1.20 in March?").expect("dip rule should parse");
        assert_eq!(dip.kind, MarketRuleKind::Below);
        assert_eq!(
            dip.upper_strike
                .expect("upper strike")
                .0
                .normalize()
                .to_string(),
            "1.2"
        );
    }

    #[test]
    fn parse_strike_price_ignores_year_when_low_strike_present() {
        let price = parse_strike_price(
            "Will Ethereum dip to $800 in March?",
            "will-ethereum-dip-to-800-in-march-2026",
        )
        .expect("price should parse");
        assert_eq!(price.0.normalize().to_string(), "800");
    }

    #[test]
    fn parse_strike_price_rejects_non_price_event_numbers() {
        assert_eq!(
            parse_strike_price(
                "Will Bitcoin replace SHA-256 before 2027?",
                "will-bitcoin-replace-sha-256-before-2027",
            ),
            None
        );
    }

    #[test]
    fn parse_strike_price_rejects_all_time_high_date_numbers() {
        assert_eq!(
            parse_strike_price(
                "Ethereum all time high by September 30, 2026?",
                "ethereum-all-time-high-by-september-30-2026",
            ),
            None
        );
    }

    #[test]
    fn resolve_assets_defaults_to_mainstream_universe() {
        let assets = resolve_assets(&[]);
        let labels = assets
            .iter()
            .map(|asset| asset.display_name)
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["BTC", "ETH", "SOL", "XRP"]);
    }

    #[test]
    fn asset_detection_avoids_ethena_and_sol_vega_false_positives() {
        let ethena_event = serde_json::json!({
            "title": "Will Ethena dip to $0 in March?",
            "slug": "will-ethena-dip-to-0-in-march",
            "markets": [{
                "question": "Will Ethena dip to $0 in March?",
                "slug": "will-ethena-dip-to-0-in-march"
            }]
        });
        let sol_vega_event = serde_json::json!({
            "title": "Will Sol Vega win Big Brother Brasil 26?",
            "slug": "will-sol-vega-win-big-brother-brasil-26",
            "markets": [{
                "question": "Will Sol Vega win Big Brother Brasil 26?",
                "slug": "will-sol-vega-win-big-brother-brasil-26"
            }]
        });

        assert!(!is_asset_event(
            &ethena_event,
            AssetSpec::from_asset(MarketAsset::Eth)
        ));
        assert!(!is_asset_event(
            &sol_vega_event,
            AssetSpec::from_asset(MarketAsset::Sol)
        ));
    }

    #[test]
    fn synthesize_market_config_reuses_template_fields() {
        let template = MarketConfig {
            symbol: Symbol::new("eth-template"),
            poly_ids: PolymarketIds {
                condition_id: "old-condition".to_owned(),
                yes_token_id: "old-yes".to_owned(),
                no_token_id: "old-no".to_owned(),
            },
            market_question: Some("template question".to_owned()),
            market_rule: None,
            cex_symbol: "ETHUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::from(3_000u32))),
            settlement_timestamp: 1_775_016_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 2),
            cex_qty_step: Decimal::new(1, 2),
            cex_contract_multiplier: Decimal::ONE,
        };
        let discovered = DiscoveredMarket {
            event_id: "evt-1".to_owned(),
            event_title: "ETH event".to_owned(),
            event_slug: "eth-event".to_owned(),
            market_id: "mkt-1".to_owned(),
            question: "Will Ethereum dip to $1,800 in March?".to_owned(),
            market_slug: "will-ethereum-dip-to-1800-in-march-2026".to_owned(),
            condition_id: "new-condition".to_owned(),
            yes_token_id: "new-yes".to_owned(),
            no_token_id: "new-no".to_owned(),
            settlement_timestamp: 1_775_016_000,
            settlement_iso: "2026-04-01T04:00:00+00:00".to_owned(),
            neg_risk: false,
            strike_price: Some(Price(Decimal::from(1_800u32))),
            market_rule: parse_market_rule("Will Ethereum dip to $1,800 in March?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(25_000.0),
            volume_num: Some(120_000.0),
            volume_24h: Some(40_000.0),
        };

        let market = synthesize_market_config(&discovered, &template, Decimal::new(1, 3))
            .expect("synthesized market");
        assert_eq!(market.symbol.0, discovered.market_slug);
        assert_eq!(market.poly_ids.condition_id, "new-condition");
        assert_eq!(market.poly_ids.yes_token_id, "new-yes");
        assert_eq!(market.poly_ids.no_token_id, "new-no");
        assert_eq!(market.cex_symbol, "ETHUSDT");
        assert_eq!(market.cex_price_tick, Decimal::new(1, 2));
        assert_eq!(market.cex_qty_step, Decimal::new(1, 3));
        assert_eq!(
            market
                .market_rule
                .expect("market rule")
                .upper_strike
                .expect("upper strike")
                .0
                .normalize()
                .to_string(),
            "1800"
        );
    }

    #[test]
    fn parse_binance_exchange_info_step_size_extracts_lot_size_step() {
        let payload = serde_json::json!({
            "symbols": [{
                "symbol": "ETHUSDT",
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.01"
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.001"
                    }
                ]
            }]
        });

        let step = parse_binance_exchange_info_step_size(&payload, "ETHUSDT")
            .expect("step size should parse");

        assert_eq!(step, Decimal::new(1, 3));
    }

    #[test]
    fn apply_synced_cex_qty_step_prefers_synced_binance_value() {
        let market = MarketConfig {
            symbol: Symbol::new("eth-template"),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some("template question".to_owned()),
            market_rule: None,
            cex_symbol: "ETHUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::from(3_000u32))),
            settlement_timestamp: 1_775_016_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 2),
            cex_qty_step: Decimal::new(1, 2),
            cex_contract_multiplier: Decimal::ONE,
        };
        let synced = HashMap::from([(
            (Exchange::Binance, "ETHUSDT".to_owned()),
            Decimal::new(1, 3),
        )]);

        let market = apply_synced_cex_qty_step(market, &synced);

        assert_eq!(market.cex_qty_step, Decimal::new(1, 3));
    }

    #[tokio::test]
    async fn fetch_binance_cex_qty_steps_reads_exchange_info_step_size() {
        let base_url = spawn_static_http_server(
            "200 OK",
            r#"{"symbols":[{"symbol":"ETHUSDT","filters":[{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}"#,
            1,
        );
        let client = Client::builder().build().expect("reqwest client");

        let steps = fetch_binance_cex_qty_steps(&client, &base_url, &[String::from("ETHUSDT")])
            .await
            .expect("exchange info fetch should succeed");

        assert_eq!(
            steps.get(&(Exchange::Binance, String::from("ETHUSDT"))),
            Some(&Decimal::new(1, 3))
        );
    }

    #[tokio::test]
    async fn fetch_binance_cex_qty_steps_hard_fails_when_exchange_info_fails() {
        let base_url = spawn_static_http_server(
            "500 Internal Server Error",
            r#"{"code":"boom"}"#,
            HTTP_GET_RETRY_ATTEMPTS,
        );
        let client = Client::builder().build().expect("reqwest client");

        let err = fetch_binance_cex_qty_steps(&client, &base_url, &[String::from("ETHUSDT")])
            .await
            .expect_err("500 exchange info should fail refresh");

        assert!(err.to_string().contains("exchangeInfo"));
    }

    #[test]
    fn refresh_catalog_market_config_replaces_legacy_symbol_with_discovered_slug() {
        let existing = MarketConfig {
            symbol: Symbol::new("eth-reach-3k"),
            poly_ids: PolymarketIds {
                condition_id: "old-condition".to_owned(),
                yes_token_id: "old-yes".to_owned(),
                no_token_id: "old-no".to_owned(),
            },
            market_question: Some("legacy question".to_owned()),
            market_rule: parse_market_rule("Will Ethereum reach $3,000 in March?"),
            cex_symbol: "ETHUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::from(3_000u32))),
            settlement_timestamp: 1_775_016_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 2),
            cex_qty_step: Decimal::new(1, 2),
            cex_contract_multiplier: Decimal::ONE,
        };
        let discovered = DiscoveredMarket {
            event_id: "evt-1".to_owned(),
            event_title: "ETH event".to_owned(),
            event_slug: "eth-event".to_owned(),
            market_id: "mkt-1".to_owned(),
            question: "Will Ethereum reach $3,600 in March?".to_owned(),
            market_slug: "will-ethereum-reach-3600-in-march".to_owned(),
            condition_id: "new-condition".to_owned(),
            yes_token_id: "new-yes".to_owned(),
            no_token_id: "new-no".to_owned(),
            settlement_timestamp: 1_775_016_001,
            settlement_iso: "2026-04-01T04:00:01+00:00".to_owned(),
            neg_risk: true,
            strike_price: Some(Price(Decimal::from(3_600u32))),
            market_rule: parse_market_rule("Will Ethereum reach $3,600 in March?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(25_000.0),
            volume_num: Some(120_000.0),
            volume_24h: Some(40_000.0),
        };

        let refreshed = refresh_catalog_market_config(existing, &discovered);
        assert_eq!(refreshed.symbol.0, discovered.market_slug);
        assert_eq!(refreshed.poly_ids.condition_id, discovered.condition_id);
        assert_eq!(refreshed.poly_ids.yes_token_id, discovered.yes_token_id);
        assert_eq!(refreshed.poly_ids.no_token_id, discovered.no_token_id);
        assert_eq!(
            refreshed.market_question.as_deref(),
            Some(discovered.question.as_str())
        );
        assert_eq!(refreshed.strike_price, discovered.strike_price);
        assert_eq!(
            refreshed.settlement_timestamp,
            discovered.settlement_timestamp
        );
        assert_eq!(refreshed.neg_risk, discovered.neg_risk);
    }

    #[test]
    fn validate_and_select_markets_filters_stale_or_dead_candidates() {
        let base_ts = (Utc::now() + chrono::Duration::days(10)).timestamp() as u64;
        let supported_rule = MarketRule {
            kind: MarketRuleKind::Above,
            lower_strike: Some(Price(Decimal::new(70_000, 0))),
            upper_strike: None,
        };
        let mk = |slug: &str,
                  accepting_orders: bool,
                  liquidity_num: f64,
                  volume_24h: f64,
                  best_bid: f64,
                  best_ask: f64| DiscoveredMarket {
            event_id: format!("evt-{slug}"),
            event_title: format!("title-{slug}"),
            event_slug: format!("event-{slug}"),
            market_id: format!("mkt-{slug}"),
            question: "Will the price of Bitcoin be above $70,000 on April 7?".to_owned(),
            market_slug: slug.to_owned(),
            condition_id: format!("cond-{slug}"),
            yes_token_id: format!("yes-{slug}"),
            no_token_id: format!("no-{slug}"),
            settlement_timestamp: base_ts,
            settlement_iso: "2030-01-01T00:00:00Z".to_owned(),
            neg_risk: false,
            strike_price: supported_rule.lower_strike,
            market_rule: Some(supported_rule.clone()),
            event_archived: false,
            market_archived: false,
            accepting_orders,
            enable_orderbook: true,
            best_bid: Some(best_bid),
            best_ask: Some(best_ask),
            liquidity_num: Some(liquidity_num),
            volume_num: Some(volume_24h * 10.0),
            volume_24h: Some(volume_24h),
        };

        let (selection, rejected) = validate_and_select_markets(
            vec![
                mk("strong-a", true, 20_000.0, 50_000.0, 0.45, 0.46),
                mk("strong-b", true, 15_000.0, 40_000.0, 0.44, 0.45),
                mk("not-accepting", false, 50_000.0, 80_000.0, 0.47, 0.48),
                mk("dead-book", true, 10.0, 0.0, 0.0, 0.0),
            ],
            MarketValidationPolicy {
                max_markets_per_asset: 2,
                min_minutes_to_settlement: 15,
                min_liquidity_usd: 1_000.0,
                min_volume_24h_usd: 1_000.0,
                market_scope: StrategyMarketScopeConfig::default(),
            },
        );

        assert_eq!(selection.validated_total, 2);
        assert_eq!(selection.target_selected, 2);
        assert_eq!(selection.truncated, 0);
        assert_eq!(
            selection
                .ranked_validated
                .iter()
                .map(|market| market.market_slug.as_str())
                .collect::<Vec<_>>(),
            vec!["strong-a", "strong-b"]
        );
        assert_eq!(rejected.len(), 2);
    }

    #[test]
    fn validate_and_select_markets_rejects_path_dependent_touch_markets() {
        let base_ts = (Utc::now() + chrono::Duration::days(10)).timestamp() as u64;
        let market = DiscoveredMarket {
            event_id: "evt-touch".to_owned(),
            event_title: "BTC touch".to_owned(),
            event_slug: "btc-touch".to_owned(),
            market_id: "mkt-touch".to_owned(),
            question: "Will Bitcoin reach $80,000 by December 31, 2026?".to_owned(),
            market_slug: "will-bitcoin-reach-80k-by-december-31-2026".to_owned(),
            condition_id: "cond-touch".to_owned(),
            yes_token_id: "yes-touch".to_owned(),
            no_token_id: "no-touch".to_owned(),
            settlement_timestamp: base_ts,
            settlement_iso: "2030-01-01T00:00:00Z".to_owned(),
            neg_risk: false,
            strike_price: parse_strike_price(
                "Will Bitcoin reach $80,000 by December 31, 2026?",
                "will-bitcoin-reach-80k-by-december-31-2026",
            ),
            market_rule: parse_market_rule("Will Bitcoin reach $80,000 by December 31, 2026?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(25_000.0),
            volume_num: Some(120_000.0),
            volume_24h: Some(40_000.0),
        };

        let (selection, rejected) = validate_and_select_markets(
            vec![market],
            MarketValidationPolicy {
                max_markets_per_asset: 8,
                min_minutes_to_settlement: 15,
                min_liquidity_usd: 1_000.0,
                min_volume_24h_usd: 1_000.0,
                market_scope: StrategyMarketScopeConfig::default(),
            },
        );

        assert_eq!(selection.validated_total, 0);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn validate_and_select_markets_rejects_all_time_high_markets() {
        let base_ts = (Utc::now() + chrono::Duration::days(10)).timestamp() as u64;
        let market = DiscoveredMarket {
            event_id: "evt-ath".to_owned(),
            event_title: "SOL ATH".to_owned(),
            event_slug: "sol-ath".to_owned(),
            market_id: "mkt-ath".to_owned(),
            question: "Solana all time high by December 31, 2026?".to_owned(),
            market_slug: "solana-all-time-high-by-december-31-2026".to_owned(),
            condition_id: "cond-ath".to_owned(),
            yes_token_id: "yes-ath".to_owned(),
            no_token_id: "no-ath".to_owned(),
            settlement_timestamp: base_ts,
            settlement_iso: "2030-01-01T00:00:00Z".to_owned(),
            neg_risk: false,
            strike_price: parse_strike_price(
                "Solana all time high by December 31, 2026?",
                "solana-all-time-high-by-december-31-2026",
            ),
            market_rule: parse_market_rule("Solana all time high by December 31, 2026?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(25_000.0),
            volume_num: Some(120_000.0),
            volume_24h: Some(40_000.0),
        };

        let (selection, rejected) = validate_and_select_markets(
            vec![market],
            MarketValidationPolicy {
                max_markets_per_asset: 8,
                min_minutes_to_settlement: 15,
                min_liquidity_usd: 1_000.0,
                min_volume_24h_usd: 1_000.0,
                market_scope: StrategyMarketScopeConfig::default(),
            },
        );

        assert_eq!(selection.validated_total, 0);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn validate_and_select_markets_rejects_non_price_event_markets() {
        let base_ts = (Utc::now() + chrono::Duration::days(10)).timestamp() as u64;
        let market = DiscoveredMarket {
            event_id: "evt-non-price".to_owned(),
            event_title: "BTC SHA".to_owned(),
            event_slug: "btc-sha".to_owned(),
            market_id: "mkt-non-price".to_owned(),
            question: "Will Bitcoin replace SHA-256 before 2027?".to_owned(),
            market_slug: "will-bitcoin-replace-sha-256-before-2027".to_owned(),
            condition_id: "cond-non-price".to_owned(),
            yes_token_id: "yes-non-price".to_owned(),
            no_token_id: "no-non-price".to_owned(),
            settlement_timestamp: base_ts,
            settlement_iso: "2030-01-01T00:00:00Z".to_owned(),
            neg_risk: false,
            strike_price: parse_strike_price(
                "Will Bitcoin replace SHA-256 before 2027?",
                "will-bitcoin-replace-sha-256-before-2027",
            ),
            market_rule: parse_market_rule("Will Bitcoin replace SHA-256 before 2027?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(25_000.0),
            volume_num: Some(120_000.0),
            volume_24h: Some(40_000.0),
        };

        let (selection, rejected) = validate_and_select_markets(
            vec![market],
            MarketValidationPolicy {
                max_markets_per_asset: 8,
                min_minutes_to_settlement: 15,
                min_liquidity_usd: 1_000.0,
                min_volume_24h_usd: 1_000.0,
                market_scope: StrategyMarketScopeConfig::default(),
            },
        );

        assert_eq!(selection.validated_total, 0);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn payload_has_nonempty_orderbook_requires_real_levels() {
        let empty = serde_json::json!({
            "bids": [],
            "asks": []
        });
        assert!(!payload_has_nonempty_orderbook(&empty));

        let populated = serde_json::json!({
            "book": {
                "bids": [{"price": "0.45", "size": "100"}],
                "asks": []
            }
        });
        let root = populated.get("book").expect("book");
        assert!(payload_has_nonempty_orderbook(root));
    }

    #[test]
    fn parse_event_lookup_payload_returns_none_when_gamma_has_no_event() {
        let payload = serde_json::json!([]);

        let parsed = parse_event_lookup_payload(payload, "343490").expect("payload should parse");

        assert!(parsed.is_none());
    }

    #[test]
    fn parse_event_lookup_payload_rejects_non_array_payloads() {
        let payload = serde_json::json!({
            "id": "343490",
            "title": "unexpected object payload"
        });

        let err = parse_event_lookup_payload(payload, "343490").expect_err("non-array payload");

        assert!(err
            .to_string()
            .contains("gamma returned non-array event payload for id 343490"));
    }

    #[test]
    fn derive_refresh_report_path_uses_report_suffix() {
        assert_eq!(
            derive_refresh_report_path(std::path::Path::new(
                "config/multi-market-active.fresh.toml"
            )),
            "config/multi-market-active.fresh.report.json"
        );
        assert_eq!(
            derive_refresh_report_path(std::path::Path::new("multi-market-active.toml")),
            "multi-market-active.report.json"
        );
    }

    #[test]
    fn ensure_unique_market_symbol_rewrites_duplicate_catalog_symbol() {
        let mut market = MarketConfig {
            symbol: Symbol::new("eth-dip-1k"),
            poly_ids: PolymarketIds {
                condition_id: "condition-duplicate-12345678".to_owned(),
                yes_token_id: "yes-dup".to_owned(),
                no_token_id: "no-dup".to_owned(),
            },
            market_question: Some("Will Ethereum dip to $1,000 by June 2026?".to_owned()),
            market_rule: parse_market_rule("Will Ethereum dip to $1,000 by June 2026?"),
            cex_symbol: "ETHUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::from(1_000u32))),
            settlement_timestamp: 1_775_016_000,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        };
        let discovered = DiscoveredMarket {
            event_id: "evt-dup".to_owned(),
            event_title: "Ethereum duplicate".to_owned(),
            event_slug: "ethereum-duplicate".to_owned(),
            market_id: "mkt-dup".to_owned(),
            question: "Will Ethereum dip to $1,000 by June 2026?".to_owned(),
            market_slug: "will-ethereum-dip-to-1000-by-june-2026".to_owned(),
            condition_id: "condition-duplicate-12345678".to_owned(),
            yes_token_id: "yes-dup".to_owned(),
            no_token_id: "no-dup".to_owned(),
            settlement_timestamp: 1_775_016_000,
            settlement_iso: "2026-04-01T04:00:00+00:00".to_owned(),
            neg_risk: false,
            strike_price: Some(Price(Decimal::from(1_000u32))),
            market_rule: parse_market_rule("Will Ethereum dip to $1,000 by June 2026?"),
            event_archived: false,
            market_archived: false,
            accepting_orders: true,
            enable_orderbook: true,
            best_bid: Some(0.45),
            best_ask: Some(0.46),
            liquidity_num: Some(20_000.0),
            volume_num: Some(100_000.0),
            volume_24h: Some(25_000.0),
        };
        let mut used_symbols = HashSet::from([market.symbol.0.clone()]);

        let (output_symbol, rewritten) =
            ensure_unique_market_symbol(&mut market, &discovered, &mut used_symbols);

        assert!(rewritten);
        assert_eq!(output_symbol, discovered.market_slug);
        assert_eq!(market.symbol.0, discovered.market_slug);
    }

    #[test]
    fn generated_settings_roundtrip_through_toml() {
        let settings = Settings {
            general: GeneralConfig {
                log_level: "info".to_owned(),
                data_dir: "./data".to_owned(),
                monitor_socket_path: "/tmp/polyalpha.sock".to_owned(),
            },
            polymarket: PolymarketConfig {
                clob_api_url: "https://clob.polymarket.com".to_owned(),
                ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
                chain_id: 137,
                private_key: None,
                signature_type: None,
                funder: None,
                api_key_nonce: None,
                use_server_time: true,
            },
            binance: BinanceConfig {
                rest_url: "https://fapi.binance.com".to_owned(),
                ws_url: "wss://fstream.binance.com".to_owned(),
            },
            okx: OkxConfig {
                rest_url: "https://www.okx.com".to_owned(),
                ws_public_url: "wss://ws.okx.com:8443/ws/v5/public".to_owned(),
                ws_private_url: "wss://ws.okx.com:8443/ws/v5/private".to_owned(),
            },
            markets: vec![MarketConfig {
                symbol: Symbol::new("btc-reach-80k-mar"),
                poly_ids: PolymarketIds {
                    condition_id: "condition-1".to_owned(),
                    yes_token_id: "yes-1".to_owned(),
                    no_token_id: "no-1".to_owned(),
                },
                market_question: Some("Will Bitcoin reach $80,000 in March?".to_owned()),
                market_rule: parse_market_rule("Will Bitcoin reach $80,000 in March?"),
                cex_symbol: "BTCUSDT".to_owned(),
                hedge_exchange: Exchange::Binance,
                strike_price: Some(Price(Decimal::from(80_000u32))),
                settlement_timestamp: 1_775_016_000,
                min_tick_size: Price(Decimal::new(1, 2)),
                neg_risk: false,
                cex_price_tick: Decimal::new(1, 1),
                cex_qty_step: Decimal::new(1, 3),
                cex_contract_multiplier: Decimal::ONE,
            }],
            strategy: StrategyConfig {
                basis: polyalpha_core::BasisStrategyConfig {
                    entry_z_score_threshold: Decimal::new(40, 1),
                    exit_z_score_threshold: Decimal::new(5, 1),
                    rolling_window_secs: 36_000,
                    min_warmup_samples: 600,
                    min_basis_bps: Decimal::new(500, 1),
                    max_position_usd: UsdNotional(Decimal::from(200u32)),
                    max_open_instant_loss_pct_of_budget: Decimal::new(4, 2),
                    delta_rebalance_threshold: Decimal::new(5, 2),
                    delta_rebalance_interval_secs: 60,
                    band_policy: polyalpha_core::BandPolicyMode::ConfiguredBand,
                    min_poly_price: Some(Decimal::new(20, 2)),
                    max_poly_price: Some(Decimal::new(50, 2)),
                    max_data_age_minutes: 1,
                    max_time_diff_minutes: 1,
                    enable_freshness_check: true,
                    reject_on_disconnect: true,
                    overrides: HashMap::new(),
                },
                dmm: polyalpha_core::DmmStrategyConfig {
                    gamma: Decimal::new(1, 1),
                    sigma_window_secs: 300,
                    max_inventory: UsdNotional(Decimal::from(5_000u32)),
                    order_refresh_secs: 10,
                    num_levels: 3,
                    level_spacing_bps: Decimal::new(100, 1),
                    min_spread_bps: Decimal::new(200, 1),
                },
                negrisk: NegRiskStrategyConfig {
                    min_arb_bps: Decimal::new(300, 1),
                    max_legs: 8,
                    enable_inventory_backed_short: false,
                },
                settlement: polyalpha_core::SettlementRules::default(),
                market_scope: StrategyMarketScopeConfig::default(),
                market_data: MarketDataConfig {
                    mode: polyalpha_core::MarketDataMode::Ws,
                    binance_combined_ws_enabled: false,
                    max_stale_ms: 5_000,
                    planner_depth_levels: 5,
                    poly_open_max_quote_age_ms: Some(3_000),
                    cex_open_max_quote_age_ms: Some(1_000),
                    close_max_quote_age_ms: Some(5_000),
                    max_cross_leg_skew_ms: Some(2_500),
                    borderline_poly_quote_age_ms: Some(1_500),
                    reconnect_backoff_ms: 1_000,
                    snapshot_refresh_secs: 300,
                    funding_poll_secs: 60,
                },
            },
            risk: RiskConfig {
                max_total_exposure_usd: UsdNotional(Decimal::from(10_000u32)),
                max_single_position_usd: UsdNotional(Decimal::from(200u32)),
                max_daily_loss_usd: UsdNotional(Decimal::from(500u32)),
                max_drawdown_pct: Decimal::new(50, 1),
                max_open_orders: 50,
                circuit_breaker_cooldown_secs: 300,
                rate_limit_orders_per_sec: 5,
                max_persistence_lag_secs: 10,
            },
            paper: PaperTradingConfig::default(),
            paper_slippage: PaperSlippageConfig::default(),
            execution_costs: polyalpha_core::ExecutionCostConfig::default(),
            audit: AuditConfig::default(),
        };
        let payload = render_generated_settings(
            &settings,
            "multi-market-active",
            "all-markets",
            &[AssetSpec::from_asset(MarketAsset::Btc)],
            &[],
            MarketValidationPolicy {
                max_markets_per_asset: 64,
                min_minutes_to_settlement: 30,
                min_liquidity_usd: 1_000.0,
                min_volume_24h_usd: 1_000.0,
                market_scope: StrategyMarketScopeConfig::default(),
            },
        )
        .expect("render generated settings");
        let parsed: Settings = toml::from_str(&payload).expect("parse generated settings");
        assert_eq!(parsed.markets.len(), settings.markets.len());
        assert_eq!(
            parsed.strategy.market_data.mode,
            settings.strategy.market_data.mode
        );
    }
}
