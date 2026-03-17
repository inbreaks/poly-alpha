use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct Price(pub Decimal);

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct Probability(pub Decimal);

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct UsdNotional(pub Decimal);

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PolyShares(pub Decimal);

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct CexBaseQty(pub Decimal);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VenueQuantity {
    PolyShares(PolyShares),
    CexBaseQty(CexBaseQty),
}

impl Default for VenueQuantity {
    fn default() -> Self {
        Self::PolyShares(PolyShares::default())
    }
}

impl Price {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub fn to_f64(self) -> f64 {
        self.0.to_f64().unwrap_or(0.0)
    }

    pub fn from_f64_rounded(v: f64, tick_size: Decimal) -> Self {
        let d = Decimal::from_f64(v).unwrap_or(Decimal::ZERO);
        Self((d / tick_size).floor() * tick_size)
    }
}

impl Probability {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub fn to_f64(self) -> f64 {
        self.0.to_f64().unwrap_or(0.0)
    }

    pub fn from_f64_clamped(v: f64) -> Self {
        if !v.is_finite() {
            return Self::ZERO;
        }

        let d = Decimal::from_f64(v).unwrap_or(Decimal::ZERO);
        Self(d.max(Decimal::ZERO).min(Decimal::ONE))
    }
}

impl PolyShares {
    pub const ZERO: Self = Self(Decimal::ZERO);

    pub fn to_usd_notional(self, avg_price: Price) -> UsdNotional {
        UsdNotional(self.0 * avg_price.0)
    }

    /// Binary option delta = d(option_price_per_share) / dS, so shares * delta
    /// directly produces the required base-asset hedge quantity.
    pub fn to_cex_base_qty(self, delta: f64) -> CexBaseQty {
        let d = Decimal::from_f64(delta.abs()).unwrap_or(Decimal::ZERO);
        CexBaseQty(self.0 * d)
    }
}

impl UsdNotional {
    pub const ZERO: Self = Self(Decimal::ZERO);

    pub fn from_poly(shares: PolyShares, avg_price: Price) -> Self {
        Self(shares.0 * avg_price.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probability_clamps_out_of_range_values() {
        assert_eq!(Probability::from_f64_clamped(-1.0), Probability::ZERO);
        assert_eq!(Probability::from_f64_clamped(2.0), Probability::ONE);
    }

    #[test]
    fn usd_notional_converts_from_poly_shares() {
        let shares = PolyShares(Decimal::new(250, 0));
        let price = Price(Decimal::new(35, 2));
        let notional = UsdNotional::from_poly(shares, price);
        assert_eq!(notional, UsdNotional(Decimal::new(8750, 2)));
    }

    #[test]
    fn poly_shares_convert_directly_to_binary_hedge_qty() {
        let shares = PolyShares(Decimal::new(1000, 0));
        let hedge_qty = shares.to_cex_base_qty(0.0001);
        assert_eq!(hedge_qty, CexBaseQty(Decimal::new(1, 1)));
    }
}
