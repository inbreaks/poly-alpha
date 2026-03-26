//! JSON 输出模式
//!
//! 直接输出 JSON 格式的状态数据，供 agent 或其他程序使用。

use anyhow::Result;
use std::time::{Duration, Instant};

use polyalpha_core::SimpleMonitorClient;

use super::filter_state_for_market;

/// 运行 JSON 流模式
pub async fn run_json_mode(
    socket_path: &str,
    interval_ms: u64,
    market_filter: Option<&str>,
) -> Result<()> {
    let client = SimpleMonitorClient::new(socket_path);
    let min_interval = Duration::from_millis(interval_ms);
    let mut last_emit: Option<Instant> = None;

    client
        .stream_states(|state| {
            if min_interval.as_millis() > 0
                && last_emit
                    .map(|instant| instant.elapsed() < min_interval)
                    .unwrap_or(false)
            {
                return;
            }
            let filtered = filter_state_for_market(state, market_filter);
            if let Ok(json) = serde_json::to_string(&filtered) {
                println!("{}", json);
                last_emit = Some(Instant::now());
            }
        })
        .await?;

    Ok(())
}

/// 运行快照模式
pub async fn run_snapshot(
    socket_path: &str,
    timeout_ms: u64,
    market_filter: Option<&str>,
) -> Result<()> {
    let client = SimpleMonitorClient::new(socket_path);

    let state = filter_state_for_market(client.get_snapshot(timeout_ms).await?, market_filter);

    let json = serde_json::to_string_pretty(&state)?;
    println!("{}", json);

    Ok(())
}
