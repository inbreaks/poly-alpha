use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::Price;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "PascalCase")]
pub enum Exchange {
    Polymarket,
    Binance,
    Deribit,
    Okx,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TokenSide {
    Yes,
    No,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MarketRuleKind {
    Above,
    Below,
    Between,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketRule {
    pub kind: MarketRuleKind,
    #[serde(default)]
    pub lower_strike: Option<Price>,
    #[serde(default)]
    pub upper_strike: Option<Price>,
}

impl MarketRule {
    pub fn fallback_above(strike_price: Price) -> Self {
        Self {
            kind: MarketRuleKind::Above,
            lower_strike: Some(strike_price),
            upper_strike: None,
        }
    }

    pub fn is_complete(&self) -> bool {
        match self.kind {
            MarketRuleKind::Above => self.lower_strike.is_some(),
            MarketRuleKind::Below => self.upper_strike.is_some(),
            MarketRuleKind::Between => self.lower_strike.is_some() && self.upper_strike.is_some(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub enum MarketPhase {
    #[default]
    Trading,
    PreSettlement {
        hours_remaining: f64,
    },
    ForceReduce {
        target_ratio: Decimal,
        hours_remaining: f64,
    },
    CloseOnly {
        hours_remaining: f64,
    },
    SettlementPending,
    Disputed,
    Resolved,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementRules {
    pub stop_new_position_hours: u64,
    pub force_reduce_hours: u64,
    pub force_reduce_target_ratio: Decimal,
    pub close_only_hours: u64,
    pub emergency_close_hours: u64,
    pub dispute_close_only: bool,
}

impl Default for SettlementRules {
    fn default() -> Self {
        Self {
            stop_new_position_hours: 24,
            force_reduce_hours: 12,
            force_reduce_target_ratio: Decimal::new(5, 1),
            close_only_hours: 6,
            emergency_close_hours: 1,
            dispute_close_only: true,
        }
    }
}

impl MarketPhase {
    pub fn from_settlement(
        settlement_timestamp: u64,
        now_timestamp: u64,
        rules: &SettlementRules,
    ) -> Self {
        let hours_remaining = (settlement_timestamp.saturating_sub(now_timestamp) as f64) / 3600.0;

        if hours_remaining <= 0.0 {
            return Self::SettlementPending;
        }
        if hours_remaining <= rules.emergency_close_hours as f64 {
            return Self::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.close_only_hours as f64 {
            return Self::CloseOnly { hours_remaining };
        }
        if hours_remaining <= rules.force_reduce_hours as f64 {
            return Self::ForceReduce {
                target_ratio: rules.force_reduce_target_ratio,
                hours_remaining,
            };
        }
        if hours_remaining <= rules.stop_new_position_hours as f64 {
            return Self::PreSettlement { hours_remaining };
        }

        Self::Trading
    }

    pub fn allows_new_positions(&self) -> bool {
        matches!(self, Self::Trading)
    }

    pub fn allows_dmm(&self) -> bool {
        matches!(self, Self::Trading | Self::PreSettlement { .. })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolymarketIds {
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketConfig {
    pub symbol: Symbol,
    #[serde(flatten)]
    pub poly_ids: PolymarketIds,
    #[serde(default)]
    pub market_question: Option<String>,
    #[serde(default)]
    pub market_rule: Option<MarketRule>,
    pub cex_symbol: String,
    pub hedge_exchange: Exchange,
    pub strike_price: Option<Price>,
    pub settlement_timestamp: u64,
    pub min_tick_size: Price,
    pub neg_risk: bool,
    pub cex_price_tick: Decimal,
    pub cex_qty_step: Decimal,
    pub cex_contract_multiplier: Decimal,
}

impl MarketConfig {
    pub fn resolved_market_rule(&self) -> Option<MarketRule> {
        self.market_rule
            .clone()
            .filter(MarketRule::is_complete)
            .or_else(|| self.strike_price.map(MarketRule::fallback_above))
    }
}

#[derive(Clone, Debug, Default)]
pub struct SymbolRegistry {
    configs: HashMap<Symbol, MarketConfig>,
    poly_token_to_symbol: HashMap<String, (Symbol, TokenSide)>,
    poly_condition_to_symbols: HashMap<String, Vec<Symbol>>,
    cex_symbol_to_symbols: HashMap<(Exchange, String), Vec<Symbol>>,
}

pub fn cex_venue_symbol(exchange: Exchange, venue_symbol: &str) -> String {
    match exchange {
        Exchange::Okx => {
            if venue_symbol.contains('-') {
                venue_symbol.to_owned()
            } else if let Some(base) = venue_symbol.strip_suffix("USDT") {
                format!("{base}-USDT-SWAP")
            } else {
                venue_symbol.to_owned()
            }
        }
        _ => venue_symbol.to_owned(),
    }
}

impl SymbolRegistry {
    pub fn new(markets: Vec<MarketConfig>) -> Self {
        let mut registry = Self::default();

        for market in markets {
            let symbol = market.symbol.clone();
            let poly_ids = market.poly_ids.clone();
            let exchange = market.hedge_exchange;
            let cex_symbol = market.cex_symbol.clone();

            registry.poly_token_to_symbol.insert(
                poly_ids.yes_token_id.clone(),
                (symbol.clone(), TokenSide::Yes),
            );
            registry.poly_token_to_symbol.insert(
                poly_ids.no_token_id.clone(),
                (symbol.clone(), TokenSide::No),
            );
            registry
                .poly_condition_to_symbols
                .entry(poly_ids.condition_id)
                .or_default()
                .push(symbol.clone());
            registry
                .cex_symbol_to_symbols
                .entry((exchange, cex_symbol))
                .or_default()
                .push(symbol.clone());
            registry.configs.insert(symbol, market);
        }

        registry
    }

    pub fn get_config(&self, symbol: &Symbol) -> Option<&MarketConfig> {
        self.configs.get(symbol)
    }

    pub fn get_poly_ids(&self, symbol: &Symbol) -> Option<&PolymarketIds> {
        self.get_config(symbol).map(|config| &config.poly_ids)
    }

    pub fn get_cex_symbol(&self, symbol: &Symbol) -> Option<&str> {
        self.get_config(symbol)
            .map(|config| config.cex_symbol.as_str())
    }

    pub fn get_tick_sizes(&self, symbol: &Symbol) -> Option<(Decimal, Decimal)> {
        self.get_config(symbol)
            .map(|config| (config.min_tick_size.0, config.cex_qty_step))
    }

    pub fn lookup_poly_asset(&self, asset_id: &str) -> Option<(Symbol, TokenSide)> {
        self.poly_token_to_symbol.get(asset_id).cloned()
    }

    pub fn lookup_poly_condition(&self, condition_id: &str) -> Option<&[Symbol]> {
        self.poly_condition_to_symbols
            .get(condition_id)
            .map(Vec::as_slice)
    }

    pub fn lookup_cex_symbols(&self, exchange: Exchange, venue_symbol: &str) -> Option<&[Symbol]> {
        self.cex_symbol_to_symbols
            .get(&(exchange, venue_symbol.to_owned()))
            .map(Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn sample_market() -> MarketConfig {
        MarketConfig {
            symbol: Symbol::new("btc-100k-mar-2026"),
            poly_ids: PolymarketIds {
                condition_id: "condition-1".to_owned(),
                yes_token_id: "yes-1".to_owned(),
                no_token_id: "no-1".to_owned(),
            },
            market_question: Some(
                "Will the price of Bitcoin be above $100,000 on March 31, 2026?".to_owned(),
            ),
            market_rule: Some(MarketRule::fallback_above(Price(Decimal::new(100_000, 0)))),
            cex_symbol: "BTCUSDT".to_owned(),
            hedge_exchange: Exchange::Binance,
            strike_price: Some(Price(Decimal::new(100_000, 0))),
            settlement_timestamp: 1_775_001_600,
            min_tick_size: Price(Decimal::new(1, 2)),
            neg_risk: false,
            cex_price_tick: Decimal::new(1, 1),
            cex_qty_step: Decimal::new(1, 3),
            cex_contract_multiplier: Decimal::ONE,
        }
    }

    #[test]
    fn registry_builds_forward_and_reverse_maps() {
        let market = sample_market();
        let symbol = market.symbol.clone();
        let registry = SymbolRegistry::new(vec![market]);

        assert_eq!(
            registry.lookup_poly_asset("yes-1"),
            Some((symbol.clone(), TokenSide::Yes))
        );
        assert_eq!(
            registry.lookup_cex_symbols(Exchange::Binance, "BTCUSDT"),
            Some(&[symbol.clone()][..])
        );
        assert_eq!(
            registry.lookup_poly_condition("condition-1"),
            Some(&[symbol][..])
        );
    }

    #[test]
    fn market_phase_transitions_follow_settlement_rules() {
        let rules = SettlementRules::default();
        let settlement = 100 * 3600;

        assert!(matches!(
            MarketPhase::from_settlement(settlement, 0, &rules),
            MarketPhase::Trading
        ));
        assert!(matches!(
            MarketPhase::from_settlement(settlement, settlement - 20 * 3600, &rules),
            MarketPhase::PreSettlement { .. }
        ));
        assert!(matches!(
            MarketPhase::from_settlement(settlement, settlement - 8 * 3600, &rules),
            MarketPhase::ForceReduce { .. }
        ));
        assert!(matches!(
            MarketPhase::from_settlement(settlement, settlement - 2 * 3600, &rules),
            MarketPhase::CloseOnly { .. }
        ));
        assert!(matches!(
            MarketPhase::from_settlement(settlement, settlement, &rules),
            MarketPhase::SettlementPending
        ));
    }

    #[test]
    fn cex_venue_symbol_normalizes_okx_swap_symbols() {
        assert_eq!(cex_venue_symbol(Exchange::Binance, "BTCUSDT"), "BTCUSDT");
        assert_eq!(cex_venue_symbol(Exchange::Okx, "BTCUSDT"), "BTC-USDT-SWAP");
        assert_eq!(
            cex_venue_symbol(Exchange::Okx, "BTC-USDT-SWAP"),
            "BTC-USDT-SWAP"
        );
    }
}
