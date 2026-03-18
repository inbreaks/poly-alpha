use std::collections::HashMap;

use rust_decimal::Decimal;

use polyalpha_core::{
    CexBaseQty, Fill, InstrumentKind, OrderSide, PolyShares, Position, PositionKey, PositionSide,
    Price, UsdNotional, VenueQuantity,
};

#[derive(Clone, Debug, Default)]
struct TrackedPosition {
    net_qty: Decimal,
    entry_price: Price,
    realized_pnl: UsdNotional,
}

#[derive(Clone, Debug, Default)]
pub struct PositionTracker {
    positions: HashMap<PositionKey, TrackedPosition>,
    daily_realized_pnl: UsdNotional,
}

impl PositionTracker {
    pub fn apply_fill(&mut self, fill: &Fill) {
        let key = PositionKey {
            exchange: fill.exchange,
            symbol: fill.symbol.clone(),
            instrument: fill.instrument,
        };
        let fill_qty = quantity_to_decimal(fill.quantity);
        if fill_qty.is_zero() {
            return;
        }

        let signed_delta = signed_qty(fill.side, fill_qty);
        let mut tracked = self.positions.remove(&key).unwrap_or_default();
        let mut realized_delta = Decimal::ZERO - fill.fee.0;
        let old_net = tracked.net_qty;

        if old_net.is_zero() || has_same_sign(old_net, signed_delta) {
            tracked.net_qty = old_net + signed_delta;
            let new_abs_qty = tracked.net_qty.abs();
            if !new_abs_qty.is_zero() {
                let weighted_notional =
                    (tracked.entry_price.0 * old_net.abs()) + (fill.price.0 * signed_delta.abs());
                tracked.entry_price = Price(weighted_notional / new_abs_qty);
            }
        } else {
            let closed_qty = old_net.abs().min(signed_delta.abs());
            if old_net.is_sign_positive() {
                realized_delta += (fill.price.0 - tracked.entry_price.0) * closed_qty;
            } else {
                realized_delta += (tracked.entry_price.0 - fill.price.0) * closed_qty;
            }

            let new_net = old_net + signed_delta;
            tracked.net_qty = new_net;
            if new_net.is_zero() {
                tracked.entry_price = Price::ZERO;
            } else if !has_same_sign(old_net, new_net) {
                tracked.entry_price = fill.price;
            }
        }

        tracked.realized_pnl.0 += realized_delta;
        self.daily_realized_pnl.0 += realized_delta;
        self.positions.insert(key, tracked);
    }

    pub fn positions_snapshot(&self) -> HashMap<PositionKey, Position> {
        self.positions
            .iter()
            .map(|(key, tracked)| {
                let abs_qty = tracked.net_qty.abs();
                let quantity = quantity_from_instrument(key.instrument, abs_qty);
                let side = if tracked.net_qty.is_zero() {
                    PositionSide::Flat
                } else if tracked.net_qty.is_sign_positive() {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                };

                let position = Position {
                    key: key.clone(),
                    side,
                    quantity,
                    entry_price: tracked.entry_price,
                    entry_notional: UsdNotional(abs_qty * tracked.entry_price.0),
                    unrealized_pnl: UsdNotional::ZERO,
                    realized_pnl: tracked.realized_pnl,
                };
                (key.clone(), position)
            })
            .collect()
    }

    pub fn net_qty_for(&self, key: &PositionKey) -> Decimal {
        self.positions
            .get(key)
            .map(|tracked| tracked.net_qty)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn entry_price_for(&self, key: &PositionKey) -> Option<Price> {
        self.positions.get(key).map(|tracked| tracked.entry_price)
    }

    pub fn symbol_exposure_usd(&self, symbol: &polyalpha_core::Symbol) -> UsdNotional {
        let exposure = self
            .positions
            .iter()
            .filter(|(key, _)| &key.symbol == symbol)
            .fold(Decimal::ZERO, |acc, (_, tracked)| {
                acc + (tracked.net_qty.abs() * tracked.entry_price.0)
            });
        UsdNotional(exposure)
    }

    pub fn total_exposure_usd(&self) -> UsdNotional {
        let total = self.positions.values().fold(Decimal::ZERO, |acc, tracked| {
            acc + (tracked.net_qty.abs() * tracked.entry_price.0)
        });
        UsdNotional(total)
    }

    pub fn daily_realized_pnl(&self) -> UsdNotional {
        self.daily_realized_pnl
    }
}

fn quantity_to_decimal(quantity: VenueQuantity) -> Decimal {
    match quantity {
        VenueQuantity::PolyShares(PolyShares(value)) => value.abs(),
        VenueQuantity::CexBaseQty(CexBaseQty(value)) => value.abs(),
    }
}

fn quantity_from_instrument(instrument: InstrumentKind, value: Decimal) -> VenueQuantity {
    match instrument {
        InstrumentKind::PolyYes | InstrumentKind::PolyNo => {
            VenueQuantity::PolyShares(PolyShares(value))
        }
        InstrumentKind::CexPerp => VenueQuantity::CexBaseQty(CexBaseQty(value)),
    }
}

fn signed_qty(side: OrderSide, qty: Decimal) -> Decimal {
    match side {
        OrderSide::Buy => qty,
        OrderSide::Sell => -qty,
    }
}

fn has_same_sign(lhs: Decimal, rhs: Decimal) -> bool {
    (lhs.is_sign_positive() && rhs.is_sign_positive())
        || (lhs.is_sign_negative() && rhs.is_sign_negative())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyalpha_core::{Exchange, OrderId, Symbol};

    fn fill(
        side: OrderSide,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        timestamp_ms: u64,
    ) -> Fill {
        Fill {
            fill_id: format!("fill-{timestamp_ms}"),
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            instrument: InstrumentKind::CexPerp,
            order_id: OrderId(format!("order-{timestamp_ms}")),
            side,
            price: Price(price),
            quantity: VenueQuantity::CexBaseQty(CexBaseQty(qty)),
            notional_usd: UsdNotional(price * qty),
            fee: UsdNotional(fee),
            is_maker: false,
            timestamp_ms,
        }
    }

    #[test]
    fn position_tracker_applies_fills_and_realized_pnl() {
        let mut tracker = PositionTracker::default();
        tracker.apply_fill(&fill(
            OrderSide::Buy,
            Decimal::new(10, 1),
            Decimal::new(1000, 1),
            Decimal::new(10, 1),
            1,
        ));
        tracker.apply_fill(&fill(
            OrderSide::Sell,
            Decimal::new(10, 1),
            Decimal::new(900, 1),
            Decimal::new(10, 1),
            2,
        ));

        let snapshot = tracker.positions_snapshot();
        let key = PositionKey {
            exchange: Exchange::Binance,
            symbol: Symbol::new("btc-100k-mar-2026"),
            instrument: InstrumentKind::CexPerp,
        };
        let position = snapshot.get(&key).expect("position should exist");

        assert_eq!(position.side, PositionSide::Flat);
        assert_eq!(
            position.quantity,
            VenueQuantity::CexBaseQty(CexBaseQty(Decimal::ZERO))
        );
        assert_eq!(
            tracker.daily_realized_pnl(),
            UsdNotional(Decimal::new(-12, 0))
        );
    }
}
