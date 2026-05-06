//! JSON 输出模式
//!
//! 直接输出 JSON 格式的状态数据，供 agent 或其他程序使用。

use anyhow::Result;
use std::io::{self, Write};
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
    let mut stdout = io::stdout();

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
                match write_json_line(&mut stdout, &json) {
                    Ok(true) => last_emit = Some(Instant::now()),
                    Ok(false) => std::process::exit(0),
                    Err(err) => {
                        eprintln!("monitor json write failed: {err}");
                        std::process::exit(1);
                    }
                }
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
    let mut stdout = io::stdout();

    let state = filter_state_for_market(client.get_snapshot(timeout_ms).await?, market_filter);

    let json = serde_json::to_string_pretty(&state)?;
    if !write_json_line(&mut stdout, &json)? {
        return Ok(());
    }

    Ok(())
}

fn write_json_line(writer: &mut impl Write, json: &str) -> io::Result<bool> {
    match writeln!(writer, "{json}") {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::BrokenPipe => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BrokenPipeWriter;

    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_json_line_appends_newline_for_normal_writer() {
        let mut buf = Vec::new();
        let written = write_json_line(&mut buf, "{\"ok\":true}").expect("write succeeds");

        assert!(written);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "{\"ok\":true}\n");
    }

    #[test]
    fn write_json_line_treats_broken_pipe_as_clean_stop() {
        let mut writer = BrokenPipeWriter;

        let written =
            write_json_line(&mut writer, "{\"ok\":true}").expect("broken pipe is tolerated");

        assert!(!written);
    }
}
