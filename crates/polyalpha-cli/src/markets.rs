use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use regex::Regex;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use polyalpha_core::{MarketConfig, MarketRule, MarketRuleKind, Price, Settings};

use crate::commands::select_market;

const GAMMA_EVENTS_URL: &str = "https://gamma-api.polymarket.com/events";
const GAMMA_PUBLIC_SEARCH_URL: &str = "https://gamma-api.polymarket.com/public-search";
const MAX_PUBLIC_SEARCH_PAGES: usize = 10;
const BTC_KEYWORDS: [&str; 2] = ["btc", "bitcoin"];

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
}

pub async fn run_discover_btc(
    env: &str,
    template_market_index: usize,
    query: &str,
    match_text: Option<&str>,
    limit: usize,
    pick: Option<usize>,
    output: Option<&str>,
) -> Result<()> {
    let settings =
        Settings::load(env).with_context(|| format!("failed to load config env `{env}`"))?;
    let template = select_market(&settings, template_market_index)?;
    let client = Client::builder()
        .user_agent("polyalpha-market-discovery/0.1")
        .build()
        .context("failed to build discovery http client")?;

    let (mut candidates, warnings) = fetch_active_btc_markets(&client, query, limit.max(1)).await?;
    if let Some(text) = match_text {
        candidates.retain(|market| market_matches(market, text));
    }

    if candidates.is_empty() {
        return Err(anyhow!(
            "no active BTC markets matched query `{query}` and match filter `{}`",
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
        let snippet = render_market_snippet(selected, &template);
        println!("{snippet}");

        if let Some(path) = output {
            write_output(path, &snippet)?;
        }
    } else {
        print_candidates(&candidates);
        println!(
            "hint: rerun with `--pick <index>` to render TOML, e.g. `polyalpha-cli markets discover-btc --match-text 100k --pick 0 --output config/live.auto.toml`"
        );
    }

    if !warnings.is_empty() {
        println!("discovery warnings:");
        for warning in warnings.iter().take(12) {
            println!("  - {warning}");
        }
        if warnings.len() > 12 {
            println!("  - ... {} more", warnings.len() - 12);
        }
    }

    Ok(())
}

async fn fetch_active_btc_markets(
    client: &Client,
    query: &str,
    limit: usize,
) -> Result<(Vec<DiscoveredMarket>, Vec<String>)> {
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    let mut seen_event_ids = HashSet::new();
    let mut seen_market_ids = HashSet::new();

    for page in 1..=MAX_PUBLIC_SEARCH_PAGES {
        let payload = http_get_json(
            client,
            GAMMA_PUBLIC_SEARCH_URL,
            &[
                ("q", query.to_owned()),
                ("events_status", "active".to_owned()),
                ("limit_per_type", limit.min(50).to_string()),
                ("page", page.to_string()),
                ("search_tags", "false".to_owned()),
                ("search_profiles", "false".to_owned()),
            ],
        )
        .await
        .with_context(|| format!("failed to fetch public-search page {}", page))?;

        let events = payload
            .get("events")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if events.is_empty() {
            break;
        }

        for summary in events {
            let event_id = string_field(&summary, "id");
            if event_id.is_empty() || !seen_event_ids.insert(event_id.clone()) {
                continue;
            }

            let full_event = fetch_event_by_id(client, &event_id).await?;
            if !is_btc_event(&full_event) {
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

    Ok((out, warnings))
}

async fn fetch_event_by_id(client: &Client, event_id: &str) -> Result<Value> {
    let payload = http_get_json(client, GAMMA_EVENTS_URL, &[("id", event_id.to_owned())])
        .await
        .with_context(|| format!("failed to fetch event {}", event_id))?;

    let event = payload
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .ok_or_else(|| anyhow!("gamma returned no event for id {}", event_id))?;
    Ok(event)
}

fn extract_markets_from_event(event: &Value, warnings: &mut Vec<String>) -> Vec<DiscoveredMarket> {
    let event_id = string_field(event, "id");
    let event_title = string_field(event, "title");
    let event_slug = string_field(event, "slug");
    let event_end = parse_dt(event.get("endDate")).or_else(|| parse_dt(event.get("endDateIso")));
    let event_active = bool_field(event, "active").unwrap_or(true);
    let event_closed = bool_field(event, "closed").unwrap_or(false);

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
        let strike_price = parse_strike_price(&question, &market_slug);
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
            market_rule: parse_market_rule(&question)
                .or_else(|| strike_price.map(MarketRule::fallback_above)),
            strike_price,
        });
    }

    out
}

async fn http_get_json(client: &Client, url: &str, params: &[(&str, String)]) -> Result<Value> {
    let response = client
        .get(url)
        .query(params)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("failed to send GET {}", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    response
        .json::<Value>()
        .await
        .with_context(|| format!("failed to decode JSON from {}", url))
}

fn print_candidates(candidates: &[DiscoveredMarket]) {
    println!("discovered active BTC markets:");
    for (idx, market) in candidates.iter().enumerate() {
        println!(
            "[{}] symbol={} settle={} strike={} market_id={} event_id={} question={}",
            idx,
            market.market_slug,
            market.settlement_iso,
            render_optional_price(&market.strike_price),
            market.market_id,
            market.event_id,
            market.question
        );
    }
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

fn render_market_snippet(discovered: &DiscoveredMarket, template: &MarketConfig) -> String {
    let mut out = String::new();
    out.push_str("# Auto-generated by `polyalpha-cli markets discover-btc`\n");
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
    parse_price_hint(slug).or_else(|| parse_price_hint(question))
}

fn parse_market_rule(question: &str) -> Option<MarketRule> {
    let normalized = question
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();

    let between =
        Regex::new(r"between\s+\$?([0-9][0-9,]*(?:\.\d+)?)\s+and\s+\$?([0-9][0-9,]*(?:\.\d+)?)")
            .ok()?;
    if let Some(captures) = between.captures(&normalized) {
        return Some(MarketRule {
            kind: MarketRuleKind::Between,
            lower_strike: Some(Price(parse_decimal_capture(captures.get(1)?.as_str())?)),
            upper_strike: Some(Price(parse_decimal_capture(captures.get(2)?.as_str())?)),
        });
    }

    let above = Regex::new(r"(above|greater than)\s+\$?([0-9][0-9,]*(?:\.\d+)?)").ok()?;
    if let Some(captures) = above.captures(&normalized) {
        return Some(MarketRule {
            kind: MarketRuleKind::Above,
            lower_strike: Some(Price(parse_decimal_capture(captures.get(2)?.as_str())?)),
            upper_strike: None,
        });
    }

    let below = Regex::new(r"(below|less than)\s+\$?([0-9][0-9,]*(?:\.\d+)?)").ok()?;
    if let Some(captures) = below.captures(&normalized) {
        return Some(MarketRule {
            kind: MarketRuleKind::Below,
            lower_strike: None,
            upper_strike: Some(Price(parse_decimal_capture(captures.get(2)?.as_str())?)),
        });
    }

    None
}

fn parse_decimal_capture(raw: &str) -> Option<Decimal> {
    Decimal::from_str(&raw.replace(',', "")).ok()
}

fn parse_price_hint(text: &str) -> Option<Price> {
    let lower = text
        .to_lowercase()
        .replace(',', " ")
        .replace('$', " ")
        .replace('?', " ");
    for token in lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        if let Some(value) = parse_numeric_token(token) {
            if value >= 1_000 {
                let decimal = Decimal::from_str(&value.to_string()).ok()?;
                return Some(Price(decimal));
            }
        }
    }
    None
}

fn parse_numeric_token(token: &str) -> Option<u64> {
    if token.is_empty() {
        return None;
    }

    if let Some(stripped) = token.strip_suffix('k') {
        let value = stripped.parse::<u64>().ok()?;
        return Some(value * 1_000);
    }
    if let Some(stripped) = token.strip_suffix('m') {
        let value = stripped.parse::<u64>().ok()?;
        return Some(value * 1_000_000);
    }
    token.parse::<u64>().ok()
}

fn render_optional_price(price: &Option<Price>) -> String {
    price
        .as_ref()
        .map(|price| price.0.normalize().to_string())
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

fn is_btc_event(event: &Value) -> bool {
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

    let haystack = fields.join(" ").to_lowercase();
    BTC_KEYWORDS
        .iter()
        .any(|keyword| haystack.contains(keyword))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
