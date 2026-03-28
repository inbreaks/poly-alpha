//! Unix Socket 客户端
//!
//! 用于 Monitor 进程，连接 Paper Trading 服务端，
//! 接收状态更新，发送命令。

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};

use super::message::{CommandKind, Message, MonitorEvent, MonitorState};
use crate::error::Result;

/// 命令请求
struct CommandRequest {
    command_id: String,
    kind: CommandKind,
    reply_tx: oneshot::Sender<Message>,
}

/// Socket 客户端
pub struct MonitorSocketClient {
    /// 服务端地址
    socket_path: String,
    /// 状态接收端
    state_rx: Option<mpsc::Receiver<MonitorState>>,
    /// 事件接收端
    event_rx: Option<mpsc::Receiver<MonitorEvent>>,
    /// 命令发送端
    command_tx: Option<mpsc::Sender<CommandRequest>>,
    /// 连接任务句柄
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl MonitorSocketClient {
    /// 创建新的客户端
    pub fn new(socket_path: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            state_rx: None,
            event_rx: None,
            command_tx: None,
            handle: None,
        }
    }

    /// 获取默认 socket 路径
    pub fn default_socket_path() -> String {
        std::env::var("POLYALPHA__GENERAL__MONITOR_SOCKET_PATH")
            .unwrap_or_else(|_| "/tmp/polyalpha.sock".to_owned())
    }

    /// 获取状态接收端
    pub fn take_state_receiver(&mut self) -> Option<mpsc::Receiver<MonitorState>> {
        self.state_rx.take()
    }

    /// 获取事件接收端
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<MonitorEvent>> {
        self.event_rx.take()
    }

    /// 发送命令并等待回复
    pub async fn send_command(&self, kind: CommandKind) -> Result<oneshot::Receiver<Message>> {
        self.send_command_with_id(uuid::Uuid::new_v4().to_string(), kind)
            .await
    }

    /// 使用指定命令 ID 发送命令并等待回复
    pub async fn send_command_with_id(
        &self,
        command_id: String,
        kind: CommandKind,
    ) -> Result<oneshot::Receiver<Message>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if let Some(ref tx) = self.command_tx {
            tx.send(CommandRequest {
                command_id,
                kind,
                reply_tx,
            })
            .await
            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
        } else {
            return Err(crate::error::CoreError::Generic(
                "Not connected".to_string(),
            ));
        }

        Ok(reply_rx)
    }

    /// 连接并运行
    pub async fn connect(&mut self) -> Result<()> {
        let socket_path = self.socket_path.clone();

        let (state_tx, state_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(128);
        let (command_tx, command_rx) = mpsc::channel(16);

        self.state_rx = Some(state_rx);
        self.event_rx = Some(event_rx);
        self.command_tx = Some(command_tx);

        let handle = tokio::spawn(async move {
            run_connection_loop(socket_path, state_tx, event_tx, command_rx).await;
        });

        self.handle = Some(handle);

        Ok(())
    }

    /// 断开连接
    pub fn disconnect(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        self.state_rx = None;
        self.event_rx = None;
        self.command_tx = None;
    }
}

/// 处理连接
async fn run_connection_loop(
    socket_path: String,
    state_tx: mpsc::Sender<MonitorState>,
    event_tx: mpsc::Sender<MonitorEvent>,
    mut command_rx: mpsc::Receiver<CommandRequest>,
) {
    loop {
        match UnixStream::connect(&socket_path).await {
            Ok(stream) => {
                tracing::info!("Connected to monitor socket: {}", socket_path);
                if let Err(err) =
                    handle_connection(stream, &state_tx, &event_tx, &mut command_rx).await
                {
                    tracing::error!("Connection error: {}", err);
                }
                tracing::warn!("Monitor socket disconnected, reconnecting: {}", socket_path);
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to connect to {}, retrying... ({})",
                    socket_path,
                    err
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn handle_connection(
    stream: UnixStream,
    state_tx: &mpsc::Sender<MonitorState>,
    event_tx: &mpsc::Sender<MonitorEvent>,
    command_rx: &mut mpsc::Receiver<CommandRequest>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut current_state: Option<MonitorState> = None;

    // 用于跟踪待回复的命令
    let mut pending_replies: std::collections::HashMap<String, oneshot::Sender<Message>> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
            // 接收服务端消息
            read_result = reader.read_line(&mut line) => {
                match read_result {
                    Ok(0) => {
                        tracing::info!("Server disconnected");
                        break;
                    }
                    Ok(_) => {
                        if let Ok(msg) = Message::from_bytes(line.as_bytes()) {
                            match msg {
                                Message::StateUpdate { state, .. } => {
                                    current_state = Some(state.clone());
                                    let _ = state_tx.send(state).await;
                                }
                                Message::Event { event, .. } => {
                                    let _ = event_tx.try_send(event.clone());
                                    if let Some(state) =
                                        apply_event_message(&mut current_state, event)
                                    {
                                        let _ = state_tx.send(state).await;
                                    }
                                }
                                Message::CommandAck { ref command_id, .. } => {
                                    // 查找并回复等待的命令
                                    if let Some(reply_tx) = pending_replies.remove(command_id) {
                                        let _ = reply_tx.send(msg);
                                    }
                                }
                                _ => {}
                            }
                        }
                        line.clear();
                    }
                    Err(e) => {
                        tracing::error!("Read error: {}", e);
                        break;
                    }
                }
            }

            // 发送命令
            maybe_req = command_rx.recv() => {
                let Some(req) = maybe_req else {
                    return Ok(());
                };

                let msg = Message::Command {
                    command_id: req.command_id.clone(),
                    kind: req.kind,
                };
                pending_replies.insert(req.command_id, req.reply_tx);

                let bytes = msg.to_bytes().map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

/// 简单的客户端，用于测试和 JSON 模式
pub struct SimpleMonitorClient {
    socket_path: String,
}

impl SimpleMonitorClient {
    pub fn new(socket_path: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// 获取一次状态快照
    pub async fn get_snapshot(&self, timeout_ms: u64) -> Result<MonitorState> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;

        // Keep both halves - don't drop the writer or server will detect EOF
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                drop(writer);
                return Err(crate::error::CoreError::Generic("Timeout".to_string()));
            }

            let result = tokio::time::timeout(remaining, reader.read_line(&mut line))
                .await
                .map_err(|_| crate::error::CoreError::Generic("Timeout".to_string()))?;

            match result {
                Ok(0) => {
                    drop(writer);
                    return Err(crate::error::CoreError::Generic(
                        "Server disconnected".to_string(),
                    ));
                }
                Ok(_) => {
                    let msg = Message::from_bytes(line.as_bytes())
                        .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
                    line.clear();

                    match msg {
                        Message::StateUpdate { state, .. } => {
                            drop(writer);
                            return Ok(state);
                        }
                        Message::Event { .. } => continue,
                        _ => {
                            drop(writer);
                            return Err(crate::error::CoreError::Generic(
                                "Unexpected message type".to_string(),
                            ));
                        }
                    }
                }
                Err(e) => {
                    drop(writer);
                    return Err(crate::error::CoreError::Generic(e.to_string()));
                }
            }
        }
    }

    /// 流式接收状态
    pub async fn stream_states(&self, mut callback: impl FnMut(MonitorState) + Send) -> Result<()> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;

        // Keep both halves - don't drop the writer or server will detect EOF
        let (reader, _writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let mut current_state: Option<MonitorState> = None;

        loop {
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if let Ok(msg) = Message::from_bytes(line.as_bytes()) {
                        match msg {
                            Message::StateUpdate { state, .. } => {
                                current_state = Some(state.clone());
                                callback(state);
                            }
                            Message::Event { event, .. } => {
                                if let Some(state) = apply_event_message(&mut current_state, event)
                                {
                                    callback(state);
                                }
                            }
                            _ => {}
                        }
                    }
                    line.clear();
                }
                Err(e) => {
                    tracing::error!("Read error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }
}

fn apply_event_message(
    current_state: &mut Option<MonitorState>,
    event: MonitorEvent,
) -> Option<MonitorState> {
    let state = current_state.as_mut()?;
    apply_monitor_event_to_state(state, event);
    Some(state.clone())
}

fn apply_monitor_event_to_state(state: &mut MonitorState, event: MonitorEvent) {
    state.timestamp_ms = state.timestamp_ms.max(event.timestamp_ms);
    state.recent_events.insert(0, event);
    state.recent_events.truncate(20);
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

    use super::*;
    use crate::{CommandStatus, EventKind, MonitorEvent, MonitorSocketServer};
    use tokio::{io::AsyncWriteExt, net::UnixListener};

    fn unique_socket_path(name: &str) -> PathBuf {
        let short_id = uuid::Uuid::new_v4().simple().to_string();
        PathBuf::from(format!("/tmp/pa-{name}-{}.sock", &short_id[..8]))
    }

    async fn wait_for_socket(path: &PathBuf) {
        for _ in 0..50 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("socket was not created in time: {}", path.display());
    }

    #[test]
    fn event_messages_update_recent_events_and_trim_to_limit() {
        let mut state = MonitorState::default();
        state.timestamp_ms = 10;
        for idx in 0..20 {
            state.recent_events.push(MonitorEvent {
                schema_version: crate::PLANNING_SCHEMA_VERSION,
                timestamp_ms: idx,
                kind: EventKind::System,
                market: Some(format!("market-{idx}")),
                correlation_id: None,
                summary: format!("event-{idx}"),
                details: None,
                payload: None,
            });
        }

        apply_monitor_event_to_state(
            &mut state,
            MonitorEvent {
                schema_version: crate::PLANNING_SCHEMA_VERSION,
                timestamp_ms: 99,
                kind: EventKind::Risk,
                market: Some("target".to_owned()),
                correlation_id: Some("corr-1".to_owned()),
                summary: "latest".to_owned(),
                details: Some(HashMap::from([("原因".to_owned(), "测试".to_owned())])),
                payload: None,
            },
        );

        assert_eq!(state.timestamp_ms, 99);
        assert_eq!(state.recent_events.len(), 20);
        assert_eq!(state.recent_events[0].summary, "latest");
        assert_eq!(state.recent_events[0].market.as_deref(), Some("target"));
        assert_eq!(state.recent_events[19].summary, "event-18");
    }

    #[test]
    fn event_messages_are_ignored_before_initial_state() {
        let event = MonitorEvent {
            schema_version: crate::PLANNING_SCHEMA_VERSION,
            timestamp_ms: 99,
            kind: EventKind::System,
            market: None,
            correlation_id: None,
            summary: "orphan".to_owned(),
            details: None,
            payload: None,
        };
        assert!(apply_event_message(&mut None, event).is_none());
    }

    #[tokio::test]
    async fn client_receives_dedicated_events_and_updates_state_stream() {
        let socket_path = unique_socket_path("event-chain");
        let initial_state = MonitorState {
            timestamp_ms: 10,
            ..MonitorState::default()
        };
        let current_state = Arc::new(std::sync::RwLock::new(Some(initial_state.clone())));

        let server = MonitorSocketServer::new_with_current_state(
            socket_path.to_string_lossy().into_owned(),
            Arc::clone(&current_state),
        );
        let event_broadcaster = server.event_broadcaster();
        let server_task = tokio::spawn(async move {
            let _ = server
                .run(Box::new(|command_id, kind| {
                    Ok(Message::CommandAck {
                        command_id,
                        kind,
                        status: CommandStatus::Success,
                        success: true,
                        message: None,
                        error_code: None,
                        finished: true,
                        timed_out: false,
                        cancellable: false,
                    })
                }))
                .await;
        });

        wait_for_socket(&socket_path).await;

        let mut client = MonitorSocketClient::new(socket_path.to_string_lossy().into_owned());
        client.connect().await.unwrap();

        let mut state_rx = client.take_state_receiver().unwrap();
        let mut event_rx = client.take_event_receiver().unwrap();

        let first_state = tokio::time::timeout(Duration::from_secs(1), state_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_state.timestamp_ms, 10);

        let event = MonitorEvent {
            schema_version: crate::PLANNING_SCHEMA_VERSION,
            timestamp_ms: 99,
            kind: EventKind::Trade,
            market: Some("sol-dip-80-mar".to_owned()),
            correlation_id: Some("corr-1".to_owned()),
            summary: "latest event".to_owned(),
            details: Some(HashMap::from([("side".to_owned(), "buy".to_owned())])),
            payload: None,
        };
        event_broadcaster.send(event.clone()).unwrap();

        let received_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received_event.summary, "latest event");
        assert_eq!(received_event.market.as_deref(), Some("sol-dip-80-mar"));

        let updated_state = tokio::time::timeout(Duration::from_secs(1), state_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated_state.timestamp_ms, 99);
        assert_eq!(updated_state.recent_events[0].summary, "latest event");

        client.disconnect();
        server_task.abort();
        let _ = std::fs::remove_file(&socket_path);
    }

    #[tokio::test]
    async fn snapshot_ignores_events_until_state_arrives() {
        let socket_path = unique_socket_path("snapshot");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let expected_state = MonitorState {
            timestamp_ms: 42,
            ..MonitorState::default()
        };
        let expected_timestamp = expected_state.timestamp_ms;

        let server_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let event = Message::Event {
                timestamp_ms: 1,
                event: MonitorEvent {
                    schema_version: crate::PLANNING_SCHEMA_VERSION,
                    timestamp_ms: 1,
                    kind: EventKind::System,
                    market: Some("sol-dip-80-mar".to_owned()),
                    correlation_id: None,
                    summary: "event before snapshot".to_owned(),
                    details: None,
                    payload: None,
                },
            };
            stream.write_all(&event.to_bytes().unwrap()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            stream
                .write_all(
                    &Message::StateUpdate {
                        timestamp_ms: expected_state.timestamp_ms,
                        state: expected_state,
                    }
                    .to_bytes()
                    .unwrap(),
                )
                .await
                .unwrap();
        });

        let client = SimpleMonitorClient::new(socket_path.to_string_lossy().into_owned());
        let snapshot = client.get_snapshot(500).await.unwrap();
        assert_eq!(snapshot.timestamp_ms, expected_timestamp);

        server_task.await.unwrap();
        let _ = std::fs::remove_file(&socket_path);
    }
}
