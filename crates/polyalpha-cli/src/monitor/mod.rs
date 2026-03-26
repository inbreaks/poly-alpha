//! Monitor 模块
//!
//! 实时监控 PolyAlpha 交易系统的 TUI 界面。

mod app;
mod json_mode;
mod state;
mod ui;

use anyhow::{bail, Result};
use polyalpha_core::MonitorState;

pub use app::run_monitor;
pub use json_mode::{run_json_mode, run_snapshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MonitorMode {
    Tui,
    Json,
    Snapshot,
}

pub(crate) fn resolve_monitor_mode(json: bool, snapshot: bool) -> Result<MonitorMode> {
    match (json, snapshot) {
        (false, false) => Ok(MonitorMode::Tui),
        (true, false) => Ok(MonitorMode::Json),
        (false, true) => Ok(MonitorMode::Snapshot),
        (true, true) => bail!("`monitor` 不能同时指定 `--json` 和 `--snapshot`"),
    }
}

pub(crate) fn filter_state_for_market(
    state: MonitorState,
    market_filter: Option<&str>,
) -> MonitorState {
    let Some(market_filter) = market_filter.filter(|filter| !filter.trim().is_empty()) else {
        return state;
    };

    state.filter_market(market_filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_monitor_mode_routes_root_flags() {
        assert_eq!(
            resolve_monitor_mode(false, false).unwrap(),
            MonitorMode::Tui
        );
        assert_eq!(
            resolve_monitor_mode(true, false).unwrap(),
            MonitorMode::Json
        );
        assert_eq!(
            resolve_monitor_mode(false, true).unwrap(),
            MonitorMode::Snapshot
        );
    }

    #[test]
    fn resolve_monitor_mode_rejects_multiple_output_modes() {
        let err = resolve_monitor_mode(true, true).unwrap_err();
        assert!(err
            .to_string()
            .contains("不能同时指定 `--json` 和 `--snapshot`"));
    }
}
