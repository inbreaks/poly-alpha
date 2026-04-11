use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;

use crate::model::PulseAsset;

#[derive(Clone, Debug, PartialEq)]
pub struct SessionHedgeIntent {
    pub session_id: String,
    pub asset: PulseAsset,
    pub target_delta_exposure: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionHedgeAttribution {
    pub session_id: String,
    pub target_delta_exposure: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HedgeReconcileOrder {
    pub asset: PulseAsset,
    pub net_target_delta: Decimal,
    pub actual_exchange_position: Decimal,
    pub order_qty: Decimal,
    pub attributions: Vec<SessionHedgeAttribution>,
}

#[derive(Clone, Debug)]
pub struct GlobalHedgeAggregator {
    pub sessions: HashMap<String, SessionHedgeIntent>,
    pub actual_positions: HashMap<PulseAsset, Decimal>,
    pub min_order_qty: Decimal,
    pub reconcile_throttle_ms: u64,
    last_reconcile_ms: HashMap<PulseAsset, u64>,
}

impl GlobalHedgeAggregator {
    pub fn new(min_order_qty: Decimal, reconcile_throttle_ms: u64) -> Self {
        Self {
            sessions: HashMap::new(),
            actual_positions: HashMap::new(),
            min_order_qty,
            reconcile_throttle_ms,
            last_reconcile_ms: HashMap::new(),
        }
    }

    pub fn upsert(
        &mut self,
        session_id: impl Into<String>,
        asset: PulseAsset,
        target_delta_exposure: Decimal,
    ) {
        let session_id = session_id.into();
        self.sessions.insert(
            session_id.clone(),
            SessionHedgeIntent {
                session_id,
                asset,
                target_delta_exposure,
            },
        );
    }

    pub fn remove(&mut self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    pub fn sync_actual_position(&mut self, asset: PulseAsset, actual_exchange_position: Decimal) {
        self.actual_positions
            .insert(asset, actual_exchange_position);
    }

    pub fn reconcile(&mut self, asset: PulseAsset) -> Option<HedgeReconcileOrder> {
        if self.reconcile_throttle_ms > 0 {
            let now_ms = current_time_ms();
            if let Some(last_ms) = self.last_reconcile_ms.get(&asset).copied() {
                if now_ms.saturating_sub(last_ms) < self.reconcile_throttle_ms {
                    return None;
                }
            }
            self.last_reconcile_ms.insert(asset, now_ms);
        }

        let attributions = self
            .sessions
            .values()
            .filter(|intent| intent.asset == asset)
            .map(|intent| SessionHedgeAttribution {
                session_id: intent.session_id.clone(),
                target_delta_exposure: intent.target_delta_exposure,
            })
            .collect::<Vec<_>>();

        let net_target_delta = attributions
            .iter()
            .fold(Decimal::ZERO, |acc, item| acc + item.target_delta_exposure);
        let actual_exchange_position = self
            .actual_positions
            .get(&asset)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let order_qty = net_target_delta - actual_exchange_position;

        if order_qty.abs() < self.min_order_qty {
            return None;
        }

        Some(HedgeReconcileOrder {
            asset,
            net_target_delta,
            actual_exchange_position,
            order_qty,
            attributions,
        })
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn global_hedge_aggregator_nets_opposing_session_targets() {
        let mut aggregator = GlobalHedgeAggregator::new(Decimal::new(1, 3), 0);
        aggregator.upsert("session-a", PulseAsset::Btc, Decimal::new(35, 2));
        aggregator.upsert("session-b", PulseAsset::Btc, Decimal::new(-20, 2));
        aggregator.sync_actual_position(PulseAsset::Btc, Decimal::new(8, 2));

        let order = aggregator.reconcile(PulseAsset::Btc).expect("net order");

        assert_eq!(order.net_target_delta, Decimal::new(15, 2));
        assert_eq!(order.actual_exchange_position, Decimal::new(8, 2));
        assert_eq!(order.order_qty, Decimal::new(7, 2));
    }
}
