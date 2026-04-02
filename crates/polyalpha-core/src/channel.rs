use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc, watch};

use crate::event::{ExecutionEvent, MarketDataEvent, RiskStateSnapshot, SystemCommand};
use crate::types::{DmmQuoteSlot, PlanningIntent, Symbol};

pub const MARKET_DATA_CAPACITY: usize = 10_240;
pub const PLANNING_INTENT_CAPACITY: usize = 64;
pub const EXECUTION_CAPACITY: usize = 256;
pub const COMMAND_CAPACITY: usize = 64;

pub type MarketDataTx = broadcast::Sender<MarketDataEvent>;
pub type MarketDataRx = broadcast::Receiver<MarketDataEvent>;
pub type DmmSignalTx = watch::Sender<DmmQuoteSlot>;
pub type DmmSignalRx = watch::Receiver<DmmQuoteSlot>;
pub type PlanningIntentTx = mpsc::Sender<PlanningIntent>;
pub type PlanningIntentRx = mpsc::Receiver<PlanningIntent>;
pub type ExecutionTx = mpsc::Sender<ExecutionEvent>;
pub type ExecutionRx = mpsc::Receiver<ExecutionEvent>;
pub type RiskStateTx = watch::Sender<RiskStateSnapshot>;
pub type RiskStateRx = watch::Receiver<RiskStateSnapshot>;
pub type CommandTx = broadcast::Sender<SystemCommand>;
pub type CommandRx = broadcast::Receiver<SystemCommand>;

pub struct ChannelBundle {
    pub market_data_tx: MarketDataTx,
    pub market_data_rx: MarketDataRx,
    pub dmm_channels: HashMap<Symbol, (DmmSignalTx, DmmSignalRx)>,
    pub planning_intent_tx: PlanningIntentTx,
    pub planning_intent_rx: PlanningIntentRx,
    pub execution_tx: ExecutionTx,
    pub execution_rx: ExecutionRx,
    pub risk_state_tx: RiskStateTx,
    pub risk_state_rx: RiskStateRx,
    pub command_tx: CommandTx,
    pub command_rx: CommandRx,
}

pub fn create_channels(symbols: &[Symbol]) -> ChannelBundle {
    let (market_data_tx, market_data_rx) = broadcast::channel(MARKET_DATA_CAPACITY);
    let dmm_channels: HashMap<Symbol, (DmmSignalTx, DmmSignalRx)> = symbols
        .iter()
        .cloned()
        .map(|symbol| {
            let (tx, rx) = watch::channel(None);
            (symbol, (tx, rx))
        })
        .collect();
    let (planning_intent_tx, planning_intent_rx) = mpsc::channel(PLANNING_INTENT_CAPACITY);
    let (execution_tx, execution_rx) = mpsc::channel(EXECUTION_CAPACITY);
    let (risk_state_tx, risk_state_rx) = watch::channel(RiskStateSnapshot::default());
    let (command_tx, command_rx) = broadcast::channel(COMMAND_CAPACITY);

    ChannelBundle {
        market_data_tx,
        market_data_rx,
        dmm_channels,
        planning_intent_tx,
        planning_intent_rx,
        execution_tx,
        execution_rx,
        risk_state_tx,
        risk_state_rx,
        command_tx,
        command_rx,
    }
}
