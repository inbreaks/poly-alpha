//! Unix Socket 服务端
//!
//! 用于 Paper Trading 进程，接收 Monitor 客户端连接，
//! 推送状态更新，接收并执行命令。

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, RwLock};

use super::message::{CommandKind, Message, MonitorEvent, MonitorState};
use crate::error::Result;

/// Socket 服务端
pub struct MonitorSocketServer {
    /// Socket 路径
    socket_path: String,
    /// 状态广播发送端
    state_tx: broadcast::Sender<MonitorState>,
    /// 事件广播发送端
    event_tx: broadcast::Sender<MonitorEvent>,
    /// 当前状态（用于首包推送，使用同步锁以便在非 async 方法中更新）
    current_state: Arc<std::sync::RwLock<Option<MonitorState>>>,
    /// 命令接收端
    command_rx: mpsc::Receiver<CommandMessage>,
    /// 命令发送端（用于其他模块发送命令）
    command_tx: mpsc::Sender<CommandMessage>,
    /// 客户端数量
    client_count: Arc<RwLock<usize>>,
}

/// 命令消息
struct CommandMessage {
    command_id: String,
    kind: CommandKind,
    reply_tx: mpsc::Sender<Message>,
}

/// 命令处理器
pub type CommandHandler =
    Box<dyn Fn(String, CommandKind) -> std::result::Result<Message, String> + Send + Sync>;

impl MonitorSocketServer {
    /// 创建新的服务端
    pub fn new(socket_path: impl Into<String>) -> Self {
        let (state_tx, _) = broadcast::channel(16);
        let (event_tx, _) = broadcast::channel(128);
        let (command_tx, command_rx) = mpsc::channel(16);
        let client_count = Arc::new(RwLock::new(0));
        let current_state = Arc::new(std::sync::RwLock::new(None));

        Self {
            socket_path: socket_path.into(),
            state_tx,
            event_tx,
            command_rx,
            command_tx,
            client_count,
            current_state,
        }
    }

    /// 创建新的服务端（使用外部提供的 current_state）
    pub fn new_with_current_state(
        socket_path: impl Into<String>,
        current_state: Arc<std::sync::RwLock<Option<MonitorState>>>,
    ) -> Self {
        let (state_tx, _) = broadcast::channel(16);
        let (event_tx, _) = broadcast::channel(128);
        let (command_tx, command_rx) = mpsc::channel(16);
        let client_count = Arc::new(RwLock::new(0));

        Self {
            socket_path: socket_path.into(),
            state_tx,
            event_tx,
            command_rx,
            command_tx,
            client_count,
            current_state,
        }
    }

    /// 获取默认 socket 路径
    pub fn default_socket_path() -> String {
        std::env::var("POLYALPHA__GENERAL__MONITOR_SOCKET_PATH")
            .unwrap_or_else(|_| "/tmp/polyalpha.sock".to_owned())
    }

    /// 获取状态广播发送端
    pub fn state_broadcaster(&self) -> broadcast::Sender<MonitorState> {
        self.state_tx.clone()
    }

    /// 获取事件广播发送端
    pub fn event_broadcaster(&self) -> broadcast::Sender<MonitorEvent> {
        self.event_tx.clone()
    }

    /// 获取客户端数量
    pub async fn client_count(&self) -> usize {
        *self.client_count.read().await
    }

    /// 广播状态更新（同步方法，可在任何上下文调用）
    pub fn broadcast_state(&self, state: MonitorState) {
        // 保存当前状态用于首包推送
        if let Ok(mut current) = self.current_state.write() {
            *current = Some(state.clone());
        }
        let _ = self.state_tx.send(state);
    }

    /// 启动服务端
    pub async fn run(mut self, command_handler: CommandHandler) -> Result<()> {
        // 清理旧的 socket 文件
        let path = Path::new(&self.socket_path);
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        // 绑定监听
        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!("Monitor socket server listening on {}", self.socket_path);

        // 命令处理任务的发送端
        let command_tx = self.command_tx.clone();
        let current_state = Arc::clone(&self.current_state);

        loop {
            tokio::select! {
                // 接受新连接
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            let state_rx = self.state_tx.subscribe();
                            let event_rx = self.event_tx.subscribe();
                            let client_count = Arc::clone(&self.client_count);
                            let command_tx = command_tx.clone();
                            let current_state = Arc::clone(&current_state);

                            *self.client_count.write().await += 1;

                            tokio::spawn(async move {
                                if let Err(e) = handle_client(
                                    stream,
                                    state_rx,
                                    event_rx,
                                    command_tx,
                                    current_state,
                                ).await {
                                    tracing::error!("Client error: {}", e);
                                }
                                *client_count.write().await -= 1;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Accept error: {}", e);
                        }
                    }
                }

                // 处理命令
                Some(cmd_msg) = self.command_rx.recv() => {
                    let result = command_handler(cmd_msg.command_id.clone(), cmd_msg.kind.clone());
                    let reply = match result {
                        Ok(msg) => msg,
                        Err(e) => Message::CommandAck {
                            command_id: cmd_msg.command_id,
                            kind: cmd_msg.kind,
                            status: super::message::CommandStatus::Failed,
                            success: false,
                            message: Some(e),
                            error_code: None,
                            finished: true,
                            timed_out: false,
                            cancellable: false,
                        },
                    };
                    let _ = cmd_msg.reply_tx.send(reply).await;
                }
            }
        }
    }
}

/// 处理客户端连接
async fn handle_client(
    stream: UnixStream,
    mut state_rx: broadcast::Receiver<MonitorState>,
    mut event_rx: broadcast::Receiver<MonitorEvent>,
    command_tx: mpsc::Sender<CommandMessage>,
    current_state: Arc<std::sync::RwLock<Option<MonitorState>>>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(4);

    // 首包：立即发送当前状态
    // 注意：必须在 await 之前释放锁
    let initial_msg = {
        let guard = current_state.read().unwrap();
        guard.as_ref().map(|state| Message::StateUpdate {
            timestamp_ms: state.timestamp_ms,
            state: state.clone(),
        })
    };

    if let Some(msg) = initial_msg {
        let bytes = msg
            .to_bytes()
            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
        if writer.write_all(&bytes).await.is_err() {
            return Ok(());
        }
    }

    loop {
        tokio::select! {
            // 接收状态更新并发送给客户端
            state_result = state_rx.recv() => {
                match state_result {
                    Ok(state) => {
                        let msg = Message::StateUpdate {
                            timestamp_ms: state.timestamp_ms,
                            state,
                        };
                        let bytes = msg
                            .to_bytes()
                            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
                        if writer.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }

            // 接收事件更新并发送给客户端
            event_result = event_rx.recv() => {
                match event_result {
                    Ok(event) => {
                        let msg = Message::Event {
                            timestamp_ms: event.timestamp_ms,
                            event,
                        };
                        let bytes = msg
                            .to_bytes()
                            .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
                        if writer.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }

            // 接收回复并发送给客户端
            Some(reply) = reply_rx.recv() => {
                let bytes = reply
                    .to_bytes()
                    .map_err(|e| crate::error::CoreError::Generic(e.to_string()))?;
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }

            // 接收客户端命令
            read_result = reader.read_line(&mut line) => {
                match read_result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if let Ok(msg) = Message::from_bytes(line.as_bytes()) {
                            if let Message::Command { command_id, kind } = msg {
                                let reply_tx = reply_tx.clone();
                                let _ = command_tx
                                    .send(CommandMessage {
                                        command_id,
                                        kind,
                                        reply_tx,
                                    })
                                    .await;
                            }
                        }
                        line.clear();
                    }
                    Err(_) => break,
                }
            }
        }
    }

    Ok(())
}

impl Drop for MonitorSocketServer {
    fn drop(&mut self) {
        // 清理 socket 文件
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
