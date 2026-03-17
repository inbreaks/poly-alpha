use serde::{Deserialize, Serialize};

use super::UsdNotional;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct EngineParams {
    pub basis_entry_zscore: Option<f64>,
    pub basis_exit_zscore: Option<f64>,
    pub max_position_usd: Option<UsdNotional>,
}
