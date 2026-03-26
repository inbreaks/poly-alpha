//! Monitor Socket 通信模块
//!
//! 提供 Paper Trading 进程和 Monitor 进程之间的 Unix Socket 通信。
//!
//! # 架构
//!
//! ```text
//! ┌─────────────────────┐      Unix Socket      ┌─────────────────────┐
//! │   Paper Trading     │ ◄──────────────────► │      Monitor        │
//! │   (Server)          │    /tmp/polyalpha.sock│     (Client)        │
//! └─────────────────────┘                       └─────────────────────┘
//! ```
//!
//! # 使用示例
//!
//! ## 服务端 (Paper Trading)
//!
//! ```rust,ignore
//! use polyalpha_core::socket::{MonitorSocketServer, MonitorState, Message, CommandKind};
//!
//! #[tokio::main]
//! async fn main() {
//!     let server = MonitorSocketServer::new("/tmp/polyalpha.sock");
//!     let broadcaster = server.state_broadcaster();
//!
//!     // 定期广播状态
//!     tokio::spawn(async move {
//!         loop {
//!             let state = MonitorState::default(); // 填充实际状态
//!             broadcaster.send(state);
//!             tokio::time::sleep(Duration::from_millis(33)).await;
//!         }
//!     });
//!
//!     // 运行服务端，处理命令
//!     server.run(|command_id, kind| {
//!         match kind {
//!             CommandKind::Pause => {
//!                 // 暂停交易
//!                 Ok(Message::CommandAck {
//!                     command_id,
//!                     kind: CommandKind::Pause,
//!                     status: polyalpha_core::CommandStatus::Success,
//!                     success: true,
//!                     message: Some("交易已暂停".to_string()),
//!                     error_code: None,
//!                     finished: true,
//!                     timed_out: false,
//!                     cancellable: false,
//!                 })
//!             }
//!             _ => Ok(Message::CommandAck {
//!                 command_id,
//!                 kind,
//!                 status: polyalpha_core::CommandStatus::Failed,
//!                 success: false,
//!                 message: Some("Unknown command".to_string()),
//!                 error_code: None,
//!                 finished: true,
//!                 timed_out: false,
//!                 cancellable: false,
//!             })
//!         }
//!     }).await?;
//! }
//! ```
//!
//! ## 客户端 (Monitor)
//!
//! ```rust,ignore
//! use polyalpha_core::socket::{MonitorSocketClient, CommandKind};
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut client = MonitorSocketClient::new("/tmp/polyalpha.sock");
//!     client.connect().await?;
//!
//!     let mut state_rx = client.take_state_receiver().unwrap();
//!
//!     // 接收状态更新
//!     while let Some(state) = state_rx.recv().await {
//!         println!("Received state: {:?}", state);
//!     }
//!
//!     // 发送命令
//!     let reply = client.send_command(CommandKind::Pause).await?;
//!     println!("Reply: {:?}", reply.await);
//! }
//! ```

mod client;
mod message;
mod server;

pub use client::*;
pub use message::*;
pub use server::*;
