use std::str::FromStr;

use regex::Regex;
use rust_decimal::Decimal;

use polyalpha_core::{MarketRule, MarketRuleKind, Price, StrategyMarketScopeConfig};

const MONEY_CAPTURE_RE: &str = r"([0-9][0-9,]*(?:\.\d+)?(?:[kmb])?)";

pub(crate) fn main_strategy_rejection_reason(
    scope: &StrategyMarketScopeConfig,
    question: Option<&str>,
    slug: &str,
    existing_rule: Option<&MarketRule>,
) -> Option<&'static str> {
    let question = question.unwrap_or("").trim();
    let slug = slug.trim();
    let combined = normalize_text(&format!("{question} {slug}"));

    if looks_like_all_time_high_market(&combined) && !scope.allow_all_time_high_markets {
        return Some("all_time_high_market_not_supported");
    }

    if looks_like_touch_market(&combined) && !scope.allow_path_dependent_touch_markets {
        return Some("path_dependent_market_not_supported");
    }

    let parsed_terminal_rule = parse_supported_terminal_market_rule(question)
        .or_else(|| parse_supported_terminal_market_rule(slug));
    let fallback_existing_rule = (question.is_empty() && slug.is_empty())
        .then(|| existing_rule.cloned().filter(MarketRule::is_complete))
        .flatten();

    if let Some(rule) = parsed_terminal_rule.or(fallback_existing_rule) {
        let allowed = match rule.kind {
            MarketRuleKind::Above => scope.allow_terminal_above_markets,
            MarketRuleKind::Below => scope.allow_terminal_below_markets,
            MarketRuleKind::Between => scope.allow_terminal_between_markets,
        };
        return (!allowed).then_some("terminal_price_market_not_supported");
    }

    if scope.allow_non_price_event_markets {
        None
    } else {
        Some("non_price_market_not_supported")
    }
}

pub(crate) fn strategy_rejection_label_zh(reason: &str) -> &'static str {
    match reason {
        "path_dependent_market_not_supported" => "触达类题型暂未纳入主策略",
        "all_time_high_market_not_supported" => "历史新高类题型暂未纳入主策略",
        "non_price_market_not_supported" => "非价格事件题暂未纳入主策略",
        "terminal_price_market_not_supported" => "该价格题型未纳入主策略",
        _ => "未纳入主策略",
    }
}

pub(crate) fn looks_like_any_price_market_text(question: &str, slug: &str) -> bool {
    let combined = normalize_text(&format!("{question} {slug}"));
    parse_supported_terminal_market_rule(question)
        .or_else(|| parse_supported_terminal_market_rule(slug))
        .is_some()
        || looks_like_touch_market(&combined)
}

pub(crate) fn parse_supported_terminal_market_rule(text: &str) -> Option<MarketRule> {
    let normalized = normalize_text(text);
    if normalized.is_empty() {
        return None;
    }

    let between = Regex::new(&format!(
        r"between\s+\$?{money}\s+and\s+\$?{money}",
        money = MONEY_CAPTURE_RE
    ))
    .ok()?;
    if let Some(captures) = between.captures(&normalized) {
        return Some(MarketRule {
            kind: MarketRuleKind::Between,
            lower_strike: Some(Price(parse_money_capture(captures.get(1)?.as_str())?)),
            upper_strike: Some(Price(parse_money_capture(captures.get(2)?.as_str())?)),
        });
    }

    let above = Regex::new(&format!(
        r"(above|greater than)\s+\$?{money}",
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
        r"(below|less than)\s+\$?{money}",
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
}

fn looks_like_touch_market(normalized: &str) -> bool {
    normalized.contains(" reach ")
        || normalized.starts_with("reach ")
        || normalized.contains(" dip to ")
        || normalized.contains(" fall to ")
        || normalized.contains(" falls to ")
        || normalized.contains(" drop to ")
        || normalized.contains(" drops to ")
        || normalized.contains(" hit ")
        || normalized.contains(" hits ")
        || normalized.contains(" touch ")
        || normalized.contains(" touches ")
}

fn looks_like_all_time_high_market(normalized: &str) -> bool {
    normalized.contains(" all time high ")
        || normalized.starts_with("all time high ")
        || normalized.contains(" ath ")
        || normalized.starts_with("ath ")
}

fn normalize_text(text: &str) -> String {
    text.to_ascii_lowercase()
        .replace('-', " ")
        .replace('?', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_money_capture(raw: &str) -> Option<Decimal> {
    let normalized = raw.replace(',', "").trim().to_ascii_lowercase();
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

#[cfg(test)]
mod tests {
    use polyalpha_core::StrategyMarketScopeConfig;

    use super::*;

    #[test]
    fn strategy_scope_rejects_touch_markets_by_default() {
        let scope = StrategyMarketScopeConfig::default();
        assert_eq!(
            main_strategy_rejection_reason(
                &scope,
                Some("Will Bitcoin reach $80,000 by December 31, 2026?"),
                "will-bitcoin-reach-80k-by-december-31-2026",
                None,
            ),
            Some("path_dependent_market_not_supported")
        );
    }

    #[test]
    fn strategy_scope_rejects_all_time_high_markets_by_default() {
        let scope = StrategyMarketScopeConfig::default();
        assert_eq!(
            main_strategy_rejection_reason(
                &scope,
                Some("Solana all time high by December 31, 2026?"),
                "solana-all-time-high-by-december-31-2026",
                None,
            ),
            Some("all_time_high_market_not_supported")
        );
    }

    #[test]
    fn strategy_scope_accepts_terminal_above_markets_by_default() {
        let scope = StrategyMarketScopeConfig::default();
        assert_eq!(
            main_strategy_rejection_reason(
                &scope,
                Some("Will the price of Bitcoin be above $70,000 on April 7?"),
                "will-the-price-of-bitcoin-be-above-70k-on-april-7",
                None,
            ),
            None
        );
    }

    #[test]
    fn strategy_scope_ignores_stale_terminal_rule_for_non_price_text() {
        let scope = StrategyMarketScopeConfig::default();
        assert_eq!(
            main_strategy_rejection_reason(
                &scope,
                Some("Will Bitcoin replace SHA-256 before 2027?"),
                "will-bitcoin-replace-sha-256-before-2027",
                Some(&MarketRule {
                    kind: MarketRuleKind::Above,
                    lower_strike: Some(Price(Decimal::from(256u32))),
                    upper_strike: None,
                }),
            ),
            Some("non_price_market_not_supported")
        );
    }
}
