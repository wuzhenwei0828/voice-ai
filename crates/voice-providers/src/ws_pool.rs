//! WS 连接池：双 lane（Asr / Tts），bounded concurrency via semaphore
//!
//! 设计动机：
//!   - DashScope GAX 是双向流，复用连接能省去频繁握手开销
//!   - ASR / TTS 通常独立使用，互不阻塞 → 拆双 lane
//!   - 客户端（adapter）通过 acquire/release 借用连接，超出 max 时阻塞
//!
//! ## 抽象
//!
//! - `WebSocketLike` trait：抽象掉 `async-tungstenite` 实际连接，方便 mock 测试
//! - `WsPool` 维护两条 lane（每条 lane 有独立 semaphore + idle list）
//! - `PooledConn` RAII 句柄：Drop 时自动 release 回 pool（或 unhealthy 时关掉）
//!
//! ## 健康检查（lazy + on-error）
//!
//! v1 不发心跳：`is_healthy()` 由 trait 实现自己跟踪"上一次 send/recv 是否成功"。
//! 任何 send/recv 失败时实现应把内部标志位置为 false；下次 release 时会被丢弃。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::debug;

use crate::codec::{encode_frame, GaxFrame, WireFormat};

// ===== 错误 =====

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("acquire timeout after {0:?}")]
    AcquireTimeout(Duration),
    #[error("pool closed")]
    Closed,
    #[error("websocket send error: {0}")]
    Send(String),
    #[error("websocket recv error: {0}")]
    Recv(String),
    #[error("websocket closed by peer")]
    ClosedByPeer,
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("invalid ws message: {0}")]
    InvalidMessage(String),
}

impl From<CodecError> for PoolError {
    fn from(e: CodecError) -> Self {
        PoolError::Recv(e.to_string())
    }
}

// 复用 codec 错误
use crate::codec::CodecError;

// ===== WS 消息类型 =====

/// 区分 text/binary 的 WS 消息。Qwen-Audio-TTS / CosyVoice / Qwen-TTS Realtime
/// 等公共 DashScope 协议用 text JSON 做控制面、binary 做音频数据。
#[derive(Debug, Clone)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
    /// 服务端发 Close（或底层 stream 收到 None）
    Close,
}

// ===== WebSocketLike trait =====

/// 把具体 WS 实现（async-tungstenite / mock）抽象成 trait，便于测试和扩展
#[async_trait]
pub trait WebSocketLike: Send {
    /// 发文本帧（JSON 控制面常用）
    async fn send_text(&mut self, text: &str) -> Result<(), PoolError>;
    /// 发二进制帧（音频 / GAX cmd+protobuf 用）
    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError>;
    /// 收一条 WS 消息（区分 text/binary）
    async fn recv_message(&mut self) -> Result<WsMessage, PoolError>;

    /// GAX 兼容：把 GaxFrame 编码为 binary 帧发出
    async fn send_frame(&mut self, frame: GaxFrame) -> Result<(), PoolError> {
        self.send_binary(encode_frame(&frame)).await
    }

    /// GAX 兼容：等 binary 帧并按 GAX 协议解（text 帧 / ping / pong 自动跳过）
    async fn recv_frame(&mut self) -> Result<GaxFrame, PoolError> {
        loop {
            match self.recv_message().await? {
                WsMessage::Binary(bytes) => {
                    match crate::codec::decode_frame(&bytes) {
                        Ok((frame, _consumed)) => return Ok(frame),
                        Err(e) => {
                            // 长度不足 / 解码失败：直接报错（不要把脏帧丢给上层）
                            return Err(PoolError::Recv(e.to_string()));
                        }
                    }
                }
                WsMessage::Text(_) => continue, // GAX 路径忽略 text 帧
                WsMessage::Close => return Err(PoolError::ClosedByPeer),
            }
        }
    }

    /// 主动关闭连接（由调用方在 release 时触发，Drop 路径不调用）
    async fn close(self: Box<Self>) -> Result<(), PoolError>;
    /// 健康检查：v1 仅看"上一次 send/recv 是否成功"
    fn is_healthy(&self) -> bool;
}

// ===== PoolConfig =====

#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// 每 lane 最大并发（同时可借出的连接数）
    pub max_connections: usize,
    /// 借连接超时：semaphore.acquire_owned() 等不到超过此时长则报错
    pub acquire_timeout: Duration,
    /// 空闲连接超时：归还时若超过此 idle 时长则直接关掉（不进入 idle list）
    pub idle_timeout: Duration,
    /// 单次握手超时（保留字段，便于以后注入到 dialer）
    pub connect_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

// ===== Lane =====

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneKind {
    Asr,
    Tts,
}

impl LaneKind {
    fn idx(self) -> usize {
        match self {
            LaneKind::Asr => 0,
            LaneKind::Tts => 1,
        }
    }
}

struct Lane {
    idle: Mutex<VecDeque<PoolEntry>>,
    sem: Arc<Semaphore>,
}

struct PoolEntry {
    conn: Box<dyn WebSocketLike>,
    created_at: Instant,
}

// ===== WsPool =====

pub struct WsPool {
    cfg: PoolConfig,
    lanes: [Lane; 2],
}

/// 拨号器：根据 lane kind 创建一个新连接。
/// 真实实现里会调 `async-tungstenite::connect_async`，mock 测试里返回 VecDeque-backed impl。
pub type Dialer = Arc<dyn Fn(LaneKind) -> BoxFuture<'static, Result<Box<dyn WebSocketLike>, PoolError>> + Send + Sync>;

impl WsPool {
    pub fn new(cfg: PoolConfig) -> Arc<Self> {
        let max = cfg.max_connections.max(1);
        Arc::new(Self {
            cfg,
            lanes: [
                Lane {
                    idle: Mutex::new(VecDeque::new()),
                    sem: Arc::new(Semaphore::new(max)),
                },
                Lane {
                    idle: Mutex::new(VecDeque::new()),
                    sem: Arc::new(Semaphore::new(max)),
                },
            ],
        })
    }

    pub fn config(&self) -> &PoolConfig {
        &self.cfg
    }

    /// 借一条 lane 上的连接。内部先从 idle 拿，拿不到再 dial 新连接；都满则等 semaphore。
    pub async fn acquire(self: &Arc<Self>, kind: LaneKind) -> Result<PooledConn, PoolError> {
        let lane = &self.lanes[kind.idx()];

        // 1. semaphore：限流
        let permit_fut = lane.sem.clone().acquire_owned();
        let permit = match tokio::time::timeout(self.cfg.acquire_timeout, permit_fut).await {
            Ok(Ok(p)) => p,
            Ok(Err(_closed)) => return Err(PoolError::Closed),
            Err(_) => return Err(PoolError::AcquireTimeout(self.cfg.acquire_timeout)),
        };

        // 2. 尝试从 idle 取一个 healthy 的（先 pop 出来，释放 lock guard 再 await）
        let popped = {
            let mut idle = lane.idle.lock();
            idle.pop_front()
        };
        if let Some(entry) = popped {
            if entry.conn.is_healthy() && entry.created_at.elapsed() < self.cfg.idle_timeout {
                debug!(target: "voice_providers.pool", lane = ?kind, "复用 idle 连接");
                return Ok(PooledConn {
                    conn: Some(entry.conn),
                    lane_idx: kind.idx(),
                    pool: self.clone(),
                    permit: Some(permit),
                });
            }
            // unhealthy 或 idle 超时 → 关掉丢弃（lock 已释放，安全 await）
            let conn = entry.conn;
            let _ = conn.close().await; // best effort
            debug!(target: "voice_providers.pool", lane = ?kind, "丢弃 unhealthy/超时 idle 连接");
        }

        // 3. 没有 idle 可复用 → 调用方需用 acquire_or_dial 拨号，或这里直接报错提示"需先注入 dialer"
        drop(permit);
        Err(PoolError::Closed)
    }

    /// 取一条连接；若 idle 空则用 dialer 新建。
    pub async fn acquire_or_dial(
        self: &Arc<Self>,
        kind: LaneKind,
        dialer: Dialer,
    ) -> Result<PooledConn, PoolError> {
        let lane = &self.lanes[kind.idx()];

        let permit_fut = lane.sem.clone().acquire_owned();
        let permit = match tokio::time::timeout(self.cfg.acquire_timeout, permit_fut).await {
            Ok(Ok(p)) => p,
            Ok(Err(_closed)) => return Err(PoolError::Closed),
            Err(_) => return Err(PoolError::AcquireTimeout(self.cfg.acquire_timeout)),
        };

        // 先 pop，释放 lock guard 再 await
        let popped = {
            let mut idle = lane.idle.lock();
            idle.pop_front()
        };
        if let Some(entry) = popped {
            if entry.conn.is_healthy() && entry.created_at.elapsed() < self.cfg.idle_timeout {
                debug!(target: "voice_providers.pool", lane = ?kind, "复用 idle 连接");
                return Ok(PooledConn {
                    conn: Some(entry.conn),
                    lane_idx: kind.idx(),
                    pool: self.clone(),
                    permit: Some(permit),
                });
            }
            let conn = entry.conn;
            let _ = conn.close().await;
        }

        // dial 新连接
        let conn = dialer(kind).await?;
        Ok(PooledConn {
            conn: Some(conn),
            lane_idx: kind.idx(),
            pool: self.clone(),
            permit: Some(permit),
        })
    }

    /// 把连接归还到 lane（unhealthy 时直接关掉不入队）。
    fn release(self: &Arc<Self>, lane_idx: usize, conn: Box<dyn WebSocketLike>, healthy: bool) {
        if !healthy {
            debug!(target: "voice_providers.pool", lane_idx, "release: 不健康，直接关闭");
            tokio::spawn(async move {
                let _ = conn.close().await;
            });
            return;
        }
        let mut idle = self.lanes[lane_idx].idle.lock();
        idle.push_back(PoolEntry {
            conn,
            created_at: Instant::now(),
        });
    }

    /// 测试 / 观测用：lane 中 idle 数量
    pub fn idle_count(&self, kind: LaneKind) -> usize {
        self.lanes[kind.idx()].idle.lock().len()
    }
}

// ===== PooledConn =====

pub struct PooledConn {
    conn: Option<Box<dyn WebSocketLike>>,
    lane_idx: usize,
    pool: Arc<WsPool>,
    /// semaphore permit：持有它期间其它 acquire 会被阻塞；drop 时释放
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PooledConn {
    /// 借引用操作底层连接
    pub fn conn_mut(&mut self) -> &mut (dyn WebSocketLike + '_) {
        // &mut Box<dyn WebSocketLike + 'static> → &mut (dyn WebSocketLike + '_)
        // 此处借用与 lifetime 推导：self.conn.as_deref_mut() 直接给 &mut dyn
        let boxed = self.conn.as_deref_mut().expect("conn already taken");
        boxed
    }

    /// 取走连接（所有权转移给调用方），典型场景：连接已损坏，调用方想自己 close。
    pub fn take(mut self) -> Box<dyn WebSocketLike> {
        self.conn.take().expect("conn already taken")
    }

    pub async fn send(&mut self, frame: GaxFrame) -> Result<(), PoolError> {
        let conn = self.conn.as_deref_mut().expect("conn already taken");
        conn.send_frame(frame).await
    }

    pub async fn recv(&mut self) -> Result<GaxFrame, PoolError> {
        let conn = self.conn.as_deref_mut().expect("conn already taken");
        conn.recv_frame().await
    }

    /// 直接发文本帧（不走 GAX 编码），给走 JSON 控制面的模型（Qwen-Audio / Fun-ASR）
    pub async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
        let conn = self.conn.as_deref_mut().expect("conn already taken");
        conn.send_text(text).await
    }

    /// 直接发二进制裸字节（不走 GAX 编码），给 PCM 音频分片
    pub async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
        let conn = self.conn.as_deref_mut().expect("conn already taken");
        conn.send_binary(bytes).await
    }

    /// 收取下一条 WS 消息（区分 Text/Binary），不经过 GAX 长度前缀解析
    pub async fn recv_message(&mut self) -> Result<WsMessage, PoolError> {
        let conn = self.conn.as_deref_mut().expect("conn already taken");
        conn.recv_message().await
    }

    pub fn is_healthy(&self) -> bool {
        match self.conn.as_ref() {
            Some(c) => c.is_healthy(),
            None => false,
        }
    }

    /// 显式归还：手动决定"healthy 还是不 healthy"。
    /// 调用方若在 send/recv 中观察到失败，应传 healthy=false 让 pool 关掉连接。
    pub fn release(mut self, healthy: bool) {
        if let Some(conn) = self.conn.take() {
            self.pool.release(self.lane_idx, conn, healthy);
        }
        // permit 自动 drop，semaphore 槽位释放
        drop(self.permit.take());
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        // RAII：drop 时把连接还回 lane（按 is_healthy 判定）
        if let Some(conn) = self.conn.take() {
            let healthy = conn.is_healthy();
            // 不能在 Drop 中 await，所以 spawn 一个 task 异步 release
            let pool = self.pool.clone();
            let lane_idx = self.lane_idx;
            tokio::spawn(async move {
                pool.release(lane_idx, conn, healthy);
            });
        }
        // permit 字段随 self 一起 drop，OwnedSemaphorePermit 自动释放 semaphore
    }
}

// ===== 辅助：把 codec 重新导出以便使用 =====

pub use crate::codec as codec_pub;

// ===== 真实 WebSocketLike 实现：async-tungstenite =====
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use futures_util::StreamExt;

use crate::codec::{decode_frame};

/// 真实 WSS 连接的 `WebSocketLike` 实现，包装 `async_tungstenite` 的
/// `WebSocketStream`。`is_healthy` 仅看"上一次 send/recv 是否成功"，
/// 与 v1 健康检查约定一致。
pub struct TungsteniteWs {
    stream: WebSocketStream<async_tungstenite::tokio::ConnectStream>,
    healthy: AtomicBool,
}

impl TungsteniteWs {
    pub fn new(
        stream: WebSocketStream<async_tungstenite::tokio::ConnectStream>,
    ) -> Self {
        Self {
            stream,
            healthy: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl WebSocketLike for TungsteniteWs {
    async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
        if let Err(e) = self.stream.send(Message::Text(text.to_string().into())).await {
            self.healthy.store(false, Ordering::SeqCst);
            return Err(PoolError::Send(e.to_string()));
        }
        Ok(())
    }

    async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
        if let Err(e) = self.stream.send(Message::Binary(bytes.into())).await {
            self.healthy.store(false, Ordering::SeqCst);
            return Err(PoolError::Send(e.to_string()));
        }
        Ok(())
    }

    async fn recv_message(&mut self) -> Result<WsMessage, PoolError> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Text(s))) => {
                    return Ok(WsMessage::Text(s.to_string()));
                }
                Some(Ok(Message::Binary(buf))) => {
                    return Ok(WsMessage::Binary(buf.to_vec()));
                }
                Some(Ok(Message::Close(_))) => {
                    self.healthy.store(false, Ordering::SeqCst);
                    return Ok(WsMessage::Close);
                }
                Some(Ok(_)) => continue, // Ping / Pong / Frame 等跳过
                Some(Err(e)) => {
                    self.healthy.store(false, Ordering::SeqCst);
                    return Err(PoolError::Recv(e.to_string()));
                }
                None => {
                    self.healthy.store(false, Ordering::SeqCst);
                    return Err(PoolError::ClosedByPeer);
                }
            }
        }
    }

    async fn recv_frame(&mut self) -> Result<GaxFrame, PoolError> {
        loop {
            match self.recv_message().await? {
                WsMessage::Binary(bytes) => {
                    match decode_frame(&bytes) {
                        Ok((frame, _consumed)) => return Ok(frame),
                        Err(e) => {
                            self.healthy.store(false, Ordering::SeqCst);
                            return Err(PoolError::Recv(e.to_string()));
                        }
                    }
                }
                WsMessage::Text(text) => {
                    // Qwen-Audio 系列模型走 JSON 文本通道（无 GAX 长度前缀）。
                    // 框架层先包成 GaxFrame（wire=Text），由 adapter.parse_event 解析 JSON 字面。
                    // 占位 cmd = RESP_TRANSCRIPT；adapter 通过 JSON header.action 区分事件类型。
                    use crate::codec::RESP_TRANSCRIPT;
                    return Ok(GaxFrame::text(RESP_TRANSCRIPT, text.into_bytes()));
                }
                WsMessage::Close => return Err(PoolError::ClosedByPeer),
            }
        }
    }

    async fn send_frame(&mut self, frame: GaxFrame) -> Result<(), PoolError> {
        // 三种 wire format 显式分派：
        //   BinaryGax：保持 GAX 4-byte 长度前缀 + cmd + payload 的原始语义
        //   Text     ：JSON 控制面 / 命令帧（run-task / finish-task），无长度前缀
        //   RawBinary：裸 PCM 音频分片，无长度前缀
        let res = match frame.wire {
            WireFormat::BinaryGax => {
                self.stream.send(Message::Binary(encode_frame(&frame).into())).await
            }
            WireFormat::Text => {
                let text = String::from_utf8_lossy(&frame.payload).into_owned();
                self.stream.send(Message::Text(text.into())).await
            }
            WireFormat::RawBinary => {
                self.stream.send(Message::Binary(frame.payload.into())).await
            }
        };
        if let Err(e) = res {
            self.healthy.store(false, Ordering::SeqCst);
            return Err(PoolError::Send(e.to_string()));
        }
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<(), PoolError> {
        let _ = self.stream.close(None).await;
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }
}

// ===== 几个 helper 用于测试 =====

/// 测试 helper（公开，供 crate 内部单元测试 + 集成测试使用）
pub mod test_helpers {
    //! 给 mock 实现用的最小辅助：encode/decode GaxFrame over bytes

    use super::*;
    use crate::codec::{decode_frame, encode_frame, GaxFrame};
    use parking_lot::Mutex as PlMutex;
    use std::collections::VecDeque as Vdq;

    /// 内存型 mock WebSocket：内部 VecDeque 模拟对端行为
    ///
    /// - `incoming_gax`  预存 GAX 二进制帧（GAX 路径用）
    /// - `incoming_ws`   预存 WS text/binary 帧（新协议用）
    /// - `outgoing_gax`  客户端发出去的 GAX 帧
    /// - `outgoing_ws`   客户端发出去的 text/binary 帧
    pub struct MockWs {
        incoming_gax: PlMutex<Vdq<GaxFrame>>,
        incoming_ws: PlMutex<Vdq<WsMessage>>,
        outgoing_gax: PlMutex<Vec<GaxFrame>>,
        outgoing_ws: PlMutex<Vec<WsMessage>>,
        healthy: std::sync::atomic::AtomicBool,
    }

    impl MockWs {
        pub fn new() -> Self {
            Self {
                incoming_gax: PlMutex::new(Vdq::new()),
                incoming_ws: PlMutex::new(Vdq::new()),
                outgoing_gax: PlMutex::new(Vec::new()),
                outgoing_ws: PlMutex::new(Vec::new()),
                healthy: std::sync::atomic::AtomicBool::new(true),
            }
        }

        /// 推一条"对端将发送的 GAX 帧"（GAX 路径用）
        pub fn push_incoming(&self, frame: GaxFrame) {
            self.incoming_gax.lock().push_back(frame);
        }

        /// 推一条"对端将发送的 WS 消息"（text/binary/Close）
        pub fn push_incoming_ws(&self, msg: WsMessage) {
            self.incoming_ws.lock().push_back(msg);
        }

        /// 拿取我们 send 出去的所有 GAX 帧
        pub fn take_outgoing(&self) -> Vec<GaxFrame> {
            std::mem::take(&mut *self.outgoing_gax.lock())
        }

        /// 拿取我们 send 出去的所有 WS 消息
        pub fn take_outgoing_ws(&self) -> Vec<WsMessage> {
            std::mem::take(&mut *self.outgoing_ws.lock())
        }

        pub fn mark_unhealthy(&self) {
            self.healthy.store(false, std::sync::atomic::Ordering::SeqCst);
        }

        pub fn outgoing_count(&self) -> usize {
            self.outgoing_gax.lock().len() + self.outgoing_ws.lock().len()
        }
    }

    impl Default for MockWs {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl WebSocketLike for MockWs {
        async fn send_text(&mut self, text: &str) -> Result<(), PoolError> {
            self.outgoing_ws.lock().push(WsMessage::Text(text.to_string()));
            Ok(())
        }

        async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), PoolError> {
            self.outgoing_ws.lock().push(WsMessage::Binary(bytes));
            Ok(())
        }

        async fn recv_message(&mut self) -> Result<WsMessage, PoolError> {
            // 优先 WS 队列；空了再用 GAX 队列（向后兼容）
            if let Some(m) = self.incoming_ws.lock().pop_front() {
                return Ok(m);
            }
            if let Some(f) = self.incoming_gax.lock().pop_front() {
                return Ok(WsMessage::Binary(encode_frame(&f)));
            }
            self.healthy.store(false, std::sync::atomic::Ordering::SeqCst);
            Err(PoolError::ClosedByPeer)
        }

        async fn send_frame(&mut self, frame: GaxFrame) -> Result<(), PoolError> {
            self.outgoing_gax.lock().push(frame);
            Ok(())
        }

        async fn recv_frame(&mut self) -> Result<GaxFrame, PoolError> {
            // 优先 GAX 队列；空了再看 WS 队列里的 binary（兼容新协议测试）
            if let Some(f) = self.incoming_gax.lock().pop_front() {
                return Ok(f);
            }
            loop {
                match self.incoming_ws.lock().pop_front() {
                    Some(WsMessage::Binary(bytes)) => match decode_frame(&bytes) {
                        Ok((f, _)) => return Ok(f),
                        Err(_) => continue,
                    },
                    Some(WsMessage::Text(_)) => continue,
                    Some(WsMessage::Close) => {
                        self.healthy.store(false, std::sync::atomic::Ordering::SeqCst);
                        return Err(PoolError::ClosedByPeer);
                    }
                    None => {
                        self.healthy.store(false, std::sync::atomic::Ordering::SeqCst);
                        return Err(PoolError::ClosedByPeer);
                    }
                }
            }
        }

        async fn close(self: Box<Self>) -> Result<(), PoolError> {
            Ok(())
        }

        fn is_healthy(&self) -> bool {
            self.healthy.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// 通过 codec 完成一次"客户端发送 → 服务端返回"循环
    pub fn round_trip_bytes(out: &[GaxFrame]) -> Vec<u8> {
        let mut buf = Vec::new();
        for chunk in out {
            buf.extend_from_slice(&encode_frame(chunk));
        }
        let mut decoded_buf = Vec::new();
        while !buf.is_empty() {
            match decode_frame(&buf) {
                Ok((f, n)) => {
                    decoded_buf.extend_from_slice(&encode_frame(&f));
                    buf.drain(..n);
                }
                Err(_) => break,
            }
        }
        decoded_buf
    }
}