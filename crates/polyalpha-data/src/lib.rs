pub mod error;
pub mod live;
pub mod manager;
pub mod mock_source;
pub mod normalizer;

pub use error::{DataError, Result};
pub use live::{BinanceFuturesDataSource, OkxMarketDataSource, PolymarketLiveDataSource};
pub use manager::DataManager;
pub use mock_source::{MockMarketDataSource, MockTick};
pub use normalizer::{
    CexBookLevel, CexBookUpdate, CexFundingUpdate, CexTradeUpdate, MarketDataNormalizer,
    PolyBookLevel, PolyBookUpdate,
};

pub fn crate_status() -> &'static str {
    "polyalpha-data normalized market data scaffolded"
}
