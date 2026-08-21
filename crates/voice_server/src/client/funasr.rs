//! FunASR 本地部署 WebSocket 客户端 —— 底层原语
//!
//! 对接本地部署的 FunASR 服务（`runtime/python/funasr_wss_server.py` 或 docker
//! `funasr-runtime-sdk` 镜像）。协议见
//! `docs/FunASR/runtime/docs/websocket_protocol_zh.md`：
//!
//!   - URL: `ws://<host>:<port>/`（本地部署常用 10095 端口，明文 ws://，无鉴权）
//!   - **WebSocket Subprotocol** —— FunASR server 在 `websockets.serve(..., subprotocols=["binary"], ...)`
//!     只接受 `binary` subprotocol，其它值会让 WS upgrade 返回 **400 Bad Request**。
//!     握手必须带 `Sec-WebSocket-Protocol: binary`。
//!   - C → S 首次（文本 JSON）：`{"mode": "2pass", "wav_name": "...", "is_speaking": true, "wav_format": "pcm", "audio_fs": 16000, ...}`
//!   - C → S 音频：裸 PCM s16le binary 帧（无任何 wrapper / header）
//!   - C → S 结束（文本 JSON）：`{"is_speaking": false}`
//!   - S → C 识别（文本 JSON）：`{"mode": "2pass-online"|"2pass-offline"|"offline", "text": "...", "is_final": ...}`
//!   - S → C 结束：服务端在所有结果输出完毕后关闭连接（**无 task-finished 等控制消息** ——
//!     仅靠 Close 帧表示本会话结束）
//!
//! Wire 协议汇总：
//!   - 协议层只暴露 WS 帧级原语：
//!     `FunasrClient::start_session`   → 建连 + 发首次 JSON（隐式 = onopen）
//!     `FunasrSender::send_audio`      → 发一段裸 PCM binary
//!     `FunasrSender::send_finish`     → 发 `{"is_speaking": false}`
//!     `FunasrReceiver::next_event`    → 阻塞收下一个 `FunasrEvent`（Message / Close / Error）
//!     `FunasrSender::close`           → 关闭连接
//!   - 编排（音频切片 / finish / 收 transcript 流）由上层（如 `live_asr_api`）负责。
//!   - `next_event` 返回 `FunasrEvent` 枚举，对应浏览器 WS 的 onmessage / onclose / onerror ——
//!     调用方 `match` 一下即可，**不再**需要记 `Ok(Some/None/Err)` 三种语义的差别。
//!   - `Close` 在 FunASR 协议下表示"识别完成"，**不是错误**（即便 code=1006 abnormal 也是退出信号）。
//!
//! 关键差异（vs Qwen GAX 协议 —— 旧的 `asr_realtime.rs` 已删除，仅作历史参考）：
//!   - 无 `header.action` envelope（flat JSON）
//!   - 无 `task_id`（会话身份仅靠 `wav_name`）
//!   - 服务端靠 Close 帧表示识别结束（**没有** task-finished 等价物）
//!   - offline 模式 `is_final` 永远为 false（语义上服务端只回一次结果，靠 Close 收尾）
//!   - 2pass-online + is_final=true = 句子边界；2pass-offline = 二次纠错结果（视为最终）

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tungstenite::{
    client::IntoClientRequest,
    protocol::CloseFrame,
    Message,
};
use async_tungstenite::tokio::{connect_async, ClientStream};
use async_tungstenite::WebSocketStream;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use crate::client::error::ClientError;

/// WS 连接类型别名
pub type WsStream = WebSocketStream<ClientStream<TcpStream>>;
pub type ArcFunasr = Arc<FunasrClient>;

// ===== 配置 =====

/// 推理模式
///
/// - `Offline` —— 离线文件转写（一次性返回结果，无流式增量）
/// - `Online` —— 实时语音识别（仅流式，无 2-pass 纠错）
/// - `TwoPass` —— 实时识别 + 句尾 2-pass 纠错（推荐；需要 2pass 模型）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunasrMode {
    Offline,
    Online,
    TwoPass,
}

impl FunasrMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            FunasrMode::Offline => "offline",
            FunasrMode::Online => "online",
            FunasrMode::TwoPass => "2pass",
        }
    }
}

/// FunASR 客户端配置
#[derive(Debug, Clone)]
pub struct FunasrConfig {
    /// WSS / WS 端点，例：`ws://127.0.0.1:10095/`（本地部署常见明文 ws://）
    pub endpoint: String,
    /// 推理模式（offline / online / 2pass）
    pub mode: FunasrMode,
    /// 音频文件名（仅用于服务端日志关联，不强制唯一）
    pub wav_name: String,
    /// 音频格式（pcm / mp3 / wav / ...）
    pub wav_format: String,
    /// PCM 采样率（FunASR 支持 8000 / 16000）
    pub sample_rate: u32,
    /// 声道数（FunASR 通常 1）
    pub channels: u16,
    /// 2pass / online 模式下的流式 latency 配置 `[左看, 当前, 右看]`（1 chunk = 60ms）；
    /// 默认 `[5, 10, 5]` = 当前 600ms，回看 300ms，前看 300ms。`None` = 不传（服务端默认）。
    pub chunk_size: Option<Vec<u32>>,
    /// 热词 JSON 字符串，例：`{"阿里巴巴":20,"通义实验室":30}`。`None` = 不传。
    /// 注：字段值是**已经 JSON 序列化的字符串**（按 docs 要求），不要二次序列化。
    pub hotwords: Option<String>,
    /// ITN（文本规范化：数字、日期等转写）。默认 `true`
    pub itn: bool,
    /// SenseVoiceSmall 模型语种。默认 `None`（让服务端走 auto）
    pub svs_lang: Option<String>,
    /// SenseVoiceSmall 是否开启标点 / ITN。默认 `true`
    pub svs_itn: bool,
    /// 附加 header（一般不用；本地部署无鉴权）
    pub extra_headers: HashMap<String, String>,
    /// send / recv 单次超时
    pub timeout: Duration,
    /// **应用层 keepalive ping 间隔**（仅在 `FunasrSender` 持有期间生效）。
    /// 后台 task 每 N 秒通过上游 WSS 发一帧 `Message::Ping(Vec::new())`，让 FunASR 服务端
    /// （Python `websockets` 库）持续收到心跳、不被自己的 idle timeout 杀掉。
    ///
    ///   - 默认 `20s` —— 与常见反向代理 / WS 服务端的 idle timeout 对齐
    ///   - `Duration::ZERO` = 禁用 keepalive
    ///
    /// 注：本参数**不**改 `next_event` 的 30s recv 超时 —— 那是"真卡了"兜底，
    /// keepalive 只防 FunASR 服务端的 idle 误杀，不掩盖协议层故障。
    pub keepalive_interval: Duration,
}

impl Default for FunasrConfig {
    fn default() -> Self {
        Self {
            endpoint: "ws://127.0.0.1:10095/".to_string(),
            mode: FunasrMode::TwoPass,
            wav_name: "default".to_string(),
            wav_format: "pcm".to_string(),
            sample_rate: 16000,
            channels: 1,
            chunk_size: Some(vec![5, 10, 5]),
            hotwords: None,
            itn: true,
            svs_lang: None,
            svs_itn: true,
            extra_headers: HashMap::new(),
            timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(20),
        }
    }
}

// ===== Client =====

/// FunASR WebSocket 客户端（长生命周期；多次 start_session 复用）
pub struct FunasrClient {
    cfg: FunasrConfig,
}

impl FunasrClient {
    pub fn new(cfg: FunasrConfig) -> Self {
        Self { cfg }
    }

    /// 构造握手请求（URL + `Sec-WebSocket-Protocol: binary` + extra_headers；本地部署无 Authorization）
    ///
    /// FunASR server `websockets.serve(..., subprotocols=["binary"], ...)` 会拒绝任何不带
    /// `Sec-WebSocket-Protocol: binary` 的握手请求（直接返回 400 Bad Request）。
    /// 这里无条件注入；如果用户 `extra_headers` 也带同名 header，后者覆盖前者。
    fn build_handshake_request(
        &self,
    ) -> Result<async_tungstenite::tungstenite::handshake::client::Request, ClientError> {
        let mut req = self
            .cfg
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|e| ClientError::Http(format!("invalid ws url: {}", e)))?;
        // 必带 Sec-WebSocket-Protocol: binary —— FunASR server 要求
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            async_tungstenite::tungstenite::http::HeaderValue::from_static("binary"),
        );
        for (k, v) in &self.cfg.extra_headers {
            if let (Ok(name), Ok(value)) = (
                async_tungstenite::tungstenite::http::HeaderName::from_bytes(k.as_bytes()),
                async_tungstenite::tungstenite::http::HeaderValue::from_str(v),
            ) {
                req.headers_mut().insert(name, value);
            }
        }
        Ok(req)
    }

    /// 建连 + 发首次 JSON 配置。返回 `FunasrSession` 供 send_audio / send_finish / recv_event。
    ///
    /// 注：FunASR 协议**没有** Qwen 那种 task-started 控制消息 —— 建连成功 = TCP+WS upgrade 通过。
    /// 后续首次 `recv_event` 通常会立刻收到一段空 + is_final=false（VAD 还没触发 / 静音段）。
    ///
    /// `session_id` 仅用于日志关联；协议层会话身份由 `cfg.wav_name` 决定。
    pub async fn start_session(&self, session_id: &str) -> Result<FunasrSession, ClientError> {
        info!(
            target: "voice_server.funasr",
            session_id,
            endpoint = %self.cfg.endpoint,
            mode = %self.cfg.mode.as_str(),
            wav_name = %self.cfg.wav_name,
            sample_rate = self.cfg.sample_rate,
            channels = self.cfg.channels,
            wav_format = %self.cfg.wav_format,
            extra_headers = self.cfg.extra_headers.len(),
            timeout_secs = self.cfg.timeout.as_secs(),
            "FunASR WSS 建连开始"
        );

        let req = self.build_handshake_request()?;
        let (ws, _resp) = connect_async(req)
            .await
            .map_err(|e| ClientError::Ws(format!("ws handshake: {}", e)))?;
        info!(
            target: "voice_server.funasr",
            session_id,
            "WSS 握手成功 (TCP+WS upgrade 通过), 即将发首次 JSON"
        );

        let (mut tx, rx) = ws.split();

        // 首次 JSON：包含 mode / wav_name / is_speaking / wav_format / audio_fs / 可选 chunk_size+hotwords 等
        let first = build_first_frame(&self.cfg);
        // WARN 级别：排查上游拒包时一定要看见这条 JSON
        warn!(
            target: "voice_server.funasr.req",
            session_id,
            wav_name = %self.cfg.wav_name,
            "→ first JSON: {}",
            first
        );
        tx.send(Message::Text(first))
            .await
            .map_err(|e| ClientError::Ws(format!("send first JSON: {}", e)))?;

        info!(
            target: "voice_server.funasr",
            session_id,
            "start_session 完成, FunasrSession 就绪 (可发 audio / recv_event)"
        );

        Ok(FunasrSession {
            tx,
            rx,
            timeout: self.cfg.timeout,
            wav_name: self.cfg.wav_name.clone(),
            keepalive_interval: self.cfg.keepalive_interval,
        })
    }
}

// ===== Session =====

/// 单个 WSS 会话（一次 start_session / 多次 send_audio / 一次 send_finish / 多次 recv_event）
///
/// **设计要点**：把 tx / rx **拆成两个独立的 half** —— `FunasrSender`（仅发）和 `FunasrReceiver`（仅收）。
/// 因为 audio 上行（on_audio_chunk）和 transcript 下行（独立后台 task）需要并发跑，
/// 不能用 `&mut FunasrSession` 互斥。先建 `FunasrSession` 再 `split()` 成两半。
pub struct FunasrSession {
    tx: SplitSink<WsStream, Message>,
    rx: SplitStream<WsStream>,
    timeout: Duration,
    wav_name: String,
    /// 转交给 `FunasrSender::spawn_keepalive` 用；`Duration::ZERO` = 禁用
    keepalive_interval: Duration,
}

impl FunasrSession {
    /// 把 WSS 流拆成发送端 + 接收端，调用方分别 spawn 进独立 task。
    ///
    /// `keepalive_interval` 由 `FunasrConfig` 传入：> 0 时会在 `FunasrSender` 内 spawn 一个
    /// 后台 task，每 N 秒向上游 WSS 发 `Message::Ping(Vec::new())`，避免 FunASR 服务端
    /// 被自己的 idle timeout 误杀（参见 `FunasrConfig::keepalive_interval` 注释）。
    pub fn split(self) -> (FunasrSender, FunasrReceiver) {
        let Self { tx, rx, timeout, wav_name, keepalive_interval } = self;
        // 共享给用户调用 + keepalive task —— 两者都要拿 `&mut SplitSink`，互斥串行
        let tx = Arc::new(TokioMutex::new(tx));
        let keepalive_handle = FunasrSender::spawn_keepalive(
            tx.clone(),
            keepalive_interval,
            wav_name.clone(),
        );
        (
            FunasrSender { tx, timeout, wav_name: wav_name.clone(), keepalive_handle },
            FunasrReceiver { rx, timeout, wav_name },
        )
    }
}

/// 发送端：send_audio / send_finish / close
/// 可被多个 task 共享（包 `Arc<Mutex<>>` 后），但调用方需自己保证互斥。
///
/// **keepalive**：构造时（`split()` 内）会按 `FunasrConfig::keepalive_interval` 起一个后台
/// ping task。`Drop` impl 会 abort 该 task —— 不会泄漏。
pub struct FunasrSender {
    tx: Arc<TokioMutex<SplitSink<WsStream, Message>>>,
    timeout: Duration,
    wav_name: String,
    keepalive_handle: Option<JoinHandle<()>>,
}

impl FunasrSender {
    /// 构造后台 keepalive task（每 `interval` 发一帧 Ping）。
    /// - `interval == ZERO` 返回 None，不 spawn
    /// - send 失败（连接已断）→ task 自然退出
    fn spawn_keepalive(
        tx: Arc<TokioMutex<SplitSink<WsStream, Message>>>,
        interval: Duration,
        wav_name: String,
    ) -> Option<JoinHandle<()>> {
        if interval.is_zero() {
            return None;
        }
        Some(tokio::spawn(async move {
            let mut tick = time::interval(interval);
            // 第一次 tick 立刻触发有点早 —— 等一个完整 interval 再开始（给业务帧让路）
            tick.tick().await;
            loop {
                tick.tick().await;
                let mut guard = tx.lock().await;
                let res = guard.send(Message::Ping(Vec::new())).await;
                match res {
                    Ok(()) => {
                        debug!(
                            target: "voice_server.funasr",
                            wav_name = %wav_name,
                            "→ keepalive Ping"
                        );
                    }
                    Err(e) => {
                        // 连接已断，task 自然退出（避免错误日志刷屏）
                        debug!(
                            target: "voice_server.funasr",
                            wav_name = %wav_name,
                            error = %e,
                            "keepalive Ping 失败，task 退出（连接已断？）"
                        );
                        break;
                    }
                }
            }
        }))
    }

    /// 发一段裸 PCM binary 帧（chunk 大小由调用方决定）
    pub async fn send_audio(&mut self, pcm: &[u8]) -> Result<(), ClientError> {
        let len = pcm.len();
        let mut guard = self.tx.lock().await;
        let res = tokio::time::timeout(self.timeout, guard.send(Message::Binary(pcm.to_vec())))
            .await
            .map_err(|_| ClientError::Ws("send_audio 超时".into()))?
            .map_err(|e| ClientError::Ws(format!("send_audio: {}", e)));
        if let Err(ref e) = res {
            warn!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                bytes = len,
                error = %e,
                "send_audio 失败"
            );
        } else {
            // debug：实时性问题排查；info 级别对 ~10 帧/秒的浏览器推流会太刷屏
            debug!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                bytes = len,
                "→ audio binary"
            );
        }
        res
    }

    /// 通知服务端本会话音频已发完：发 `{"is_speaking": false}`
    pub async fn send_finish(&mut self) -> Result<(), ClientError> {
        let frame = serde_json::to_string(&json!({"is_speaking": false}))
            .expect("static json");
        info!(
            target: "voice_server.funasr",
            wav_name = %self.wav_name,
            "→ finish JSON: {}", frame
        );
        let mut guard = self.tx.lock().await;
        let res = tokio::time::timeout(self.timeout, guard.send(Message::Text(frame)))
            .await
            .map_err(|_| ClientError::Ws("send_finish 超时".into()))?
            .map_err(|e| ClientError::Ws(format!("send_finish: {}", e)));
        if let Err(ref e) = res {
            warn!(
                target: "voice_server.funasr",
                wav_name = %self.wav_name,
                error = %e,
                "send_finish 失败"
            );
        }
        res
    }

    /// 关闭 WSS 连接（graceful Close 帧 + best-effort）。
    /// 注意：本方法**消耗** self（`mut self`），让 Drop 跑起来 —— Drop 会先 abort keepalive task，
    /// 然后 inner `Arc<TokioMutex<SplitSink>>` drop，若仍是最后一个 Arc 则 Drop::drop 触发
    /// SplitSink 的清理逻辑（向 ws stream 发 EOF）。这样保证 keepalive task 不会在 close 之后
    /// 还试图往已关闭的 stream 里塞 Ping。
    pub async fn close(mut self) -> Result<(), ClientError> {
        info!(
            target: "voice_server.funasr",
            wav_name = %self.wav_name,
            "→ WSS Close 帧（best-effort）"
        );
        let mut guard = self.tx.lock().await;
        let _ = tokio::time::timeout(self.timeout, guard.send(Message::Close(None))).await;
        // guard 在函数结尾 drop，释放锁；keepalive task 下一 tick 还会试图 lock —— 但此时
        // ws stream 已经走完 close 流程，Ping.send() 会失败 → task 自然 break 退出。
        drop(guard);
        // 显式 abort：避免 keepalive 还要再等一个 interval 才退出
        if let Some(h) = self.keepalive_handle.take() {
            h.abort();
        }
        Ok(())
    }
}

impl Drop for FunasrSender {
    fn drop(&mut self) {
        // 非 close 路径下的兜底：例如 caller 直接 `let _ = sender;` 丢掉 / 走 Arc 路径漏 close
        if let Some(h) = self.keepalive_handle.take() {
            h.abort();
        }
    }
}

/// 接收端：仅 `next_event`，由独立后台 task 持有整个生命周期。
/// FunASR 服务端在所有结果输出完毕后发 Close 帧，`next_event` 返回 `FunasrEvent::Close(_)`。
pub struct FunasrReceiver {
    rx: SplitStream<WsStream>,
    timeout: Duration,
    wav_name: String,
}

impl FunasrReceiver {
    /// 阻塞收下一个 `FunasrEvent`。
    ///   - `FunasrEvent::Message(resp)` — 识别事件（onmessage：mode + text + is_final）
    ///   - `FunasrEvent::Close(c)` — 服务端结束（onclose：FunASR 协议 = 识别完成，**不是错误**）
    ///   - `FunasrEvent::Error(e)` — WS 错误（onerror：超时 / 读失败 / stream 异常）
    ///
    /// 内部循环会**吃掉**以下帧而不返回：服务端的 metadata 帧、`Message::Ping(_)`、
    /// 解析失败的服务端 JSON（warn 后继续）。这些都不应杀掉整轮 recv。
    pub async fn next_event(&mut self) -> FunasrEvent {
        loop {
            let msg = match tokio::time::timeout(self.timeout, self.rx.next()).await {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    // WS 读错误（协议层 fail）—— onerror
                    let err = ClientError::Ws(format!("recv: {}", e));
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        error = %err,
                        "← 读 WS 帧失败（onerror）"
                    );
                    return FunasrEvent::Error(err);
                }
                Ok(None) => {
                    // stream 结束但未收到 Close 帧 —— 对齐浏览器 WS code=1006 abnormal closure
                    // **不是错误** —— 调用方应当退出 recv loop，不视作失败。
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← stream 结束但未见 Close 帧（onclose code=1006 abnormal）"
                    );
                    return FunasrEvent::Close(FunasrClose::abnormal());
                }
                Err(_) => {
                    let err = ClientError::Ws("next_event 超时".into());
                    warn!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← recv 超时（onerror）"
                    );
                    return FunasrEvent::Error(err);
                }
            };
            match msg {
                Message::Text(t) => {
                    let bytes = t.as_bytes();
                    debug!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        "← 上游文本帧: {}",
                        String::from_utf8_lossy(bytes)
                    );
                    match parse_server_event(bytes) {
                        Ok(Some(e)) => {
                            info!(
                                target: "voice_server.funasr",
                                wav_name = %self.wav_name,
                                mode = ?e.mode,
                                is_final = e.is_final,
                                text_len = e.text.chars().count(),
                                text = %e.text,
                                "← 识别结果（onmessage）"
                            );
                            return FunasrEvent::Message(e);
                        }
                        Ok(None) => continue, // 服务端偶尔发的 metadata / 未知帧，忽略继续等
                        Err(e) => {
                            // 单帧 parse 失败不能杀掉整轮 recv —— warn 后继续等下一帧
                            warn!(
                                target: "voice_server.funasr",
                                wav_name = %self.wav_name,
                                error = %e,
                                raw = %String::from_utf8_lossy(bytes),
                                "← 解析失败（继续等下一帧，不退出 loop）"
                            );
                            continue;
                        }
                    }
                }
                Message::Ping(_) => {
                    // FunASR server 配 ping_interval=None（funasr_wss_server.py:841），
                    // 理论上不会发 Ping；万一收到时丢弃即可（无 tx 不能 Pong，
                    // WebSocket 协议允许 unsent Pong —— server timeout 通常也很长）
                    debug!(target: "voice_server.funasr", wav_name = %self.wav_name, "← 收到 Ping（已忽略，FunASR server 通常不发）");
                    continue;
                }
                Message::Close(c) => {
                    // FunASR 协议下 Close = 识别完成。`Option<CloseFrame>`：服务端可能发空 Close 帧。
                    let close = c
                        .map(FunasrClose::from_frame)
                        .unwrap_or_else(FunasrClose::normal);
                    info!(
                        target: "voice_server.funasr",
                        wav_name = %self.wav_name,
                        code = close.code,
                        reason = %close.reason,
                        "← 服务端 Close 帧（onclose —— FunASR 识别结束）"
                    );
                    return FunasrEvent::Close(close);
                }
                _ => continue,
            }
        }
    }
}

// ===== 协议帧构造 =====

/// 构造首次通信 JSON（按 docs/websocket_protocol_zh.md "首次通信"）
fn build_first_frame(cfg: &FunasrConfig) -> String {
    let mut value = json!({
        "mode": cfg.mode.as_str(),
        "wav_name": cfg.wav_name,
        "is_speaking": true,
        "wav_format": cfg.wav_format,
        "audio_fs": cfg.sample_rate,
        "itn": cfg.itn,
        "svs_itn": cfg.svs_itn,
    });
    // chunk_size / hotwords / svs_lang 是 optional —— None 时跳过（不要输出 null）
    if let Some(cs) = &cfg.chunk_size {
        value["chunk_size"] = json!(cs);
    }
    if let Some(hw) = &cfg.hotwords {
        // 文档要求传字符串（已经是 JSON 序列化的字符串），不要重复 serialize
        value["hotwords"] = json!(hw);
    }
    if let Some(lang) = &cfg.svs_lang {
        value["svs_lang"] = json!(lang);
    }
    serde_json::to_string(&value).expect("static json")
}

// ===== 服务端响应解析 =====

/// FunASR 服务端响应模式（对应服务端 JSON 里的 `mode` 字段）。
///
/// - `Online` —— 实时识别流式增量（`mode: "online"`）
/// - `TwoPassOnline` —— 2pass 模式下的流式增量（`mode: "2pass-online"`）
/// - `TwoPassOffline` —— 2pass 模式下的二次纠错结果（`mode: "2pass-offline"`，仅 sentence end 后出现）
/// - `Offline` —— 离线一次性识别（`mode: "offline"`）
/// - `Other(String)` —— 未知 mode（保留原值用于日志；上层一般当 Online 处理）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunasrResponseMode {
    Online,
    TwoPassOnline,
    TwoPassOffline,
    Offline,
    Other(String),
}

impl FunasrResponseMode {
    /// FunASR 服务端的 `text` 字段在此 mode 下是"累积全文"还是其它。
    /// - 累积全文：必须做 last_text → delta 转换（前端才能"一个字一个字显示"）。
    /// - 最终纠错：必须替换上一行 final（不要 append）。
    pub fn is_cumulative(&self) -> bool {
        matches!(self, FunasrResponseMode::Online | FunasrResponseMode::TwoPassOnline)
    }
    pub fn is_correction(&self) -> bool {
        matches!(self, FunasrResponseMode::TwoPassOffline)
    }
}

/// FunASR 服务端响应解析后的内部结构（比 AsrEvent 多带 mode，供上层做累积→增量转换）
#[derive(Debug, Clone)]
pub struct FunasrResponse {
    pub mode: FunasrResponseMode,
    pub text: String,
    pub is_final: bool,
}

/// FunASR 服务端 Close 帧解析结果。
///
/// FunASR 协议下 Close **= 识别完成**，不是错误。常见取值：
///   - `code = 1000` —— 正常关闭（服务端发的 Close 帧）
///   - `code = 1006` —— abnormal closure（stream 在没收到 Close 帧时就结束了，兜底映射）
///
/// 对齐浏览器 WebSocket `onclose` 的 `code` / `reason` 字段语义。
#[derive(Debug, Clone)]
pub struct FunasrClose {
    pub code: u16,
    pub reason: String,
}

impl FunasrClose {
    /// 构造 normal closure（1000 / 空 reason）。FunASR 服务端发 Close 帧时常不带 reason。
    pub fn normal() -> Self {
        Self {
            code: 1000,
            reason: String::new(),
        }
    }
    /// 构造 abnormal closure（1006）。stream 在没收到 Close 帧就结束时用。
    pub fn abnormal() -> Self {
        Self {
            code: 1006,
            reason: "abnormal closure (no close frame)".into(),
        }
    }
    /// 从 tungstenite `CloseFrame` 构造。
    pub fn from_frame(cf: CloseFrame<'_>) -> Self {
        Self {
            code: u16::from(cf.code),
            reason: cf.reason.into_owned(),
        }
    }
}

/// `FunasrReceiver::next_event` 的返回类型 —— 对应浏览器 WebSocket 三种事件的 pull 风格。
///
/// 调用方一般 `match`：
/// ```ignore
/// match rx.next_event().await {
///     FunasrEvent::Message(resp) => { /* onmessage: 识别结果 */ }
///     FunasrEvent::Close(c)      => { /* onclose: 识别完成（FunASR 不是错误！即便 1006 也要退出 loop）*/ }
///     FunasrEvent::Error(e)      => { /* onerror: 超时 / 读失败 / 协议异常 */ }
/// }
/// ```
///
/// 设计要点：
///   - `Message` / `Close` 是协议正常事件；`Close` 在 FunASR 下 = "识别完成" —— **不是错误**。
///   - `Error` 仅承载 WS 层故障（读失败 / recv 超时 / stream 错误），不含业务逻辑错误。
///   - 解析失败（JSON 非法 / 字段缺失）的服务端帧**不上浮**为 `Error` —— `next_event` 内部
///     warn + 继续收下一帧，避免一帧坏包杀掉整轮 recv。
#[derive(Debug)]
pub enum FunasrEvent {
    /// 识别事件（onmessage）—— 服务端文本帧已 parse 成 `FunasrResponse`
    Message(FunasrResponse),
    /// 服务端结束连接（onclose）。FunASR 协议下 = 识别完成；abnormal closure (code=1006)
    /// 表示 stream 在没收到 Close 帧时就断了 —— 也**不是**错误，至少本地 recv loop 应当退出。
    Close(FunasrClose),
    /// 错误（onerror）—— WS 读失败 / recv 超时
    Error(ClientError),
}

#[derive(Debug, Deserialize)]
struct ServerResponse {
    /// `mode`：`offline` | `online` | `2pass-online` | `2pass-offline`。
    /// 服务端偶尔发的 metadata 帧可能缺该字段 —— 空字符串视为"非识别帧"，parse_server_event 返回 Ok(None)。
    #[serde(default)]
    mode: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    is_final: bool,
    #[serde(default)]
    #[allow(dead_code)]
    wav_name: String,
    #[serde(default)]
    #[allow(dead_code)]
    timestamp: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    stamp_sents: Option<serde_json::Value>,
}

fn classify_mode(raw: &str) -> FunasrResponseMode {
    match raw {
        "online" => FunasrResponseMode::Online,
        "2pass-online" => FunasrResponseMode::TwoPassOnline,
        "2pass-offline" => FunasrResponseMode::TwoPassOffline,
        "offline" => FunasrResponseMode::Offline,
        other => FunasrResponseMode::Other(other.to_string()),
    }
}

/// 解析服务端 transcript 文本帧。
///
/// 返回：
/// - `Ok(Some(resp))` — 识别事件（mode + text + is_final）
/// - `Ok(None)` — 非识别帧（mode 字段缺失或空，服务端偶尔发的 metadata）
/// - `Err(_)` — JSON 解析失败（接收方一般选择 warn + 继续 recv）
fn parse_server_event(bytes: &[u8]) -> Result<Option<FunasrResponse>, ClientError> {
    let resp: ServerResponse = serde_json::from_slice(bytes)
        .map_err(|e| ClientError::Decode(format!("decode FunASR response: {}", e)))?;
    if resp.mode.is_empty() {
        return Ok(None);
    }
    Ok(Some(FunasrResponse {
        mode: classify_mode(&resp.mode),
        text: resp.text,
        is_final: resp.is_final,
    }))
}

// ===== 工厂 =====

pub fn build_funasr_client(cfg: FunasrConfig) -> ArcFunasr {
    Arc::new(FunasrClient::new(cfg))
}

// ===== 单元测试 =====

#[cfg(test)]
mod tests {
    use super::*;

    // ===== FunasrMode =====

    #[test]
    fn mode_as_str_matches_doc() {
        // 按 docs/websocket_protocol_zh.md §首次通信 的 mode 字段值
        assert_eq!(FunasrMode::Offline.as_str(), "offline");
        assert_eq!(FunasrMode::Online.as_str(), "online");
        assert_eq!(FunasrMode::TwoPass.as_str(), "2pass");
    }

    // ===== FunasrResponseMode 分类 =====

    #[test]
    fn response_mode_classify_all_doc_values() {
        assert_eq!(classify_mode("online"), FunasrResponseMode::Online);
        assert_eq!(
            classify_mode("2pass-online"),
            FunasrResponseMode::TwoPassOnline
        );
        assert_eq!(
            classify_mode("2pass-offline"),
            FunasrResponseMode::TwoPassOffline
        );
        assert_eq!(classify_mode("offline"), FunasrResponseMode::Offline);
        match classify_mode("something-new") {
            FunasrResponseMode::Other(s) => assert_eq!(s, "something-new"),
            _ => panic!("unknown mode 应归 Other"),
        }
    }

    #[test]
    fn response_mode_is_cumulative_and_correction() {
        // online / 2pass-online → text 是累积全文，需算 delta
        assert!(FunasrResponseMode::Online.is_cumulative());
        assert!(FunasrResponseMode::TwoPassOnline.is_cumulative());
        assert!(!FunasrResponseMode::Offline.is_cumulative());
        assert!(!FunasrResponseMode::TwoPassOffline.is_cumulative());
        // 2pass-offline → 二次纠错，要 replace_last
        assert!(FunasrResponseMode::TwoPassOffline.is_correction());
        assert!(!FunasrResponseMode::Online.is_correction());
    }

    #[test]
    fn parse_response_exposes_mode() {
        // parse_server_event 必须把 mode 暴露出来（live_asr_api 累积→delta 转换需要）
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.mode, FunasrResponseMode::TwoPassOnline);
        assert_eq!(out.text, "你");
        assert!(!out.is_final);
    }

    // ===== FunasrConfig 默认值 =====

    #[test]
    fn default_config_is_local_two_pass_16k() {
        let c = FunasrConfig::default();
        assert_eq!(c.endpoint, "ws://127.0.0.1:10095/");
        assert_eq!(c.mode, FunasrMode::TwoPass);
        assert_eq!(c.wav_format, "pcm");
        assert_eq!(c.sample_rate, 16000);
        assert_eq!(c.channels, 1);
        assert_eq!(c.itn, true);
        assert_eq!(c.svs_itn, true);
        assert!(c.chunk_size.is_some());
        assert!(c.hotwords.is_none());
        assert!(c.svs_lang.is_none());
    }

    // ===== 首次通信 JSON 帧形态 =====

    #[test]
    fn first_frame_two_pass_with_hotwords_and_svs() {
        let cfg = FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            mode: FunasrMode::TwoPass,
            wav_name: "live-1".into(),
            wav_format: "pcm".into(),
            sample_rate: 16000,
            channels: 1,
            chunk_size: Some(vec![5, 10, 5]),
            hotwords: Some(r#"{"阿里巴巴":20,"通义实验室":30}"#.to_string()),
            itn: true,
            svs_lang: Some("zh".into()),
            svs_itn: true,
            extra_headers: Default::default(),
            timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(20),
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["mode"], "2pass");
        assert_eq!(v["wav_name"], "live-1");
        assert_eq!(v["is_speaking"], true);
        assert_eq!(v["wav_format"], "pcm");
        assert_eq!(v["audio_fs"], 16000);
        assert_eq!(v["chunk_size"], json!([5, 10, 5]));
        // hotwords 必须保持字符串形态（服务端要解析的 JSON 字符串，不能二次序列化）
        assert_eq!(v["hotwords"], r#"{"阿里巴巴":20,"通义实验室":30}"#);
        assert_eq!(v["svs_lang"], "zh");
        assert_eq!(v["itn"], true);
        assert_eq!(v["svs_itn"], true);
    }

    #[test]
    fn first_frame_offline_omits_chunk_size() {
        let cfg = FunasrConfig {
            mode: FunasrMode::Offline,
            chunk_size: None,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["mode"], "offline");
        // offline 模式不需要 chunk_size（音频一次性发完，无 latency 概念）
        assert!(v.get("chunk_size").is_none());
    }

    #[test]
    fn first_frame_omits_none_optional_fields() {
        let cfg = FunasrConfig {
            chunk_size: None,
            hotwords: None,
            svs_lang: None,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert!(v.get("chunk_size").is_none());
        assert!(v.get("hotwords").is_none());
        assert!(v.get("svs_lang").is_none());
    }

    #[test]
    fn first_frame_8k_for_telephony() {
        // 8kHz 是文档明确支持的采样率
        let cfg = FunasrConfig {
            sample_rate: 8000,
            ..Default::default()
        };
        let s = build_first_frame(&cfg);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["audio_fs"], 8000);
    }

    // ===== 服务端响应解析 =====

    #[test]
    fn parse_partial_2pass_online() {
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false,
            "timestamp": "[]",
            "stamp_sents": []
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_sentence_boundary_2pass_online_final() {
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好世界。",
            "is_final": true
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_2pass_offline_is_correction_result() {
        // 2pass-offline 由服务端在句子结束后补发，等价于最终文本
        let resp = json!({
            "mode": "2pass-offline",
            "wav_name": "x",
            "text": "你好世界。",
            "is_final": true
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好世界。");
        assert!(out.is_final);
    }

    #[test]
    fn parse_offline_single_response() {
        // offline 模式 is_final 文档说永远为 False（实际上服务端只发一次，靠 Close 收尾）
        let resp = json!({
            "mode": "offline",
            "wav_name": "x",
            "text": "完整文本",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "完整文本");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_response_with_timestamp_and_stamp_sents() {
        // 服务端带时间戳模型时返回 timestamp / stamp_sents，必须能容忍这两个字段
        let resp = json!({
            "mode": "2pass-online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false,
            "timestamp": "[[100,200],[200,500]]",
            "stamp_sents": [
                {"text_seg": "你", "punc": "", "start": 100, "end": 200, "ts_list": [[100,200]]}
            ]
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
    }

    #[test]
    fn parse_response_empty_mode_returns_none() {
        // 服务端偶尔会发 metadata 帧（mode 字段缺失或空），不应报错也不应产生事件
        let resp = json!({"text": "foo", "is_final": false});
        let bytes = serde_json::to_vec(&resp).unwrap();
        assert!(parse_server_event(&bytes).unwrap().is_none());
    }

    #[test]
    fn parse_invalid_json_returns_err() {
        // 单帧解析失败必须让 recv_event 知道 —— 由调用方决定 warn + 继续 vs 中断
        assert!(parse_server_event(b"not json").is_err());
    }

    #[test]
    fn parse_response_missing_text_defaults_empty() {
        // BEGIN 帧 / VAD 未触发时可能 text 字段缺失，必须能容忍
        let resp = json!({"mode": "2pass-online", "is_final": false});
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "");
        assert!(!out.is_final);
    }

    #[test]
    fn parse_response_online_mode_partial() {
        // online 模式（非 2pass）的增量帧
        let resp = json!({
            "mode": "online",
            "wav_name": "x",
            "text": "你好",
            "is_final": false
        });
        let bytes = serde_json::to_vec(&resp).unwrap();
        let out = parse_server_event(&bytes).unwrap().unwrap();
        assert_eq!(out.text, "你好");
        assert!(!out.is_final);
    }

    // ===== 握手请求 =====

    #[test]
    fn handshake_request_accepts_plain_ws_scheme() {
        // 本地部署常用 ws://（明文），必须能解析 URL —— 之前如果某处只接受 wss:// 就会爆
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(
            req.uri().scheme_str(),
            Some("ws"),
            "本地 FunASR 必须是 ws scheme"
        );
    }

    #[test]
    fn handshake_request_accepts_wss_scheme() {
        // 通过反向代理暴露时可能用 wss://
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "wss://funasr.example.com/ws".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(req.uri().scheme_str(), Some("wss"));
    }

    #[test]
    fn handshake_request_invalid_url_errors() {
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "not a url".into(),
            ..Default::default()
        });
        assert!(client.build_handshake_request().is_err());
    }

    #[test]
    fn handshake_request_applies_extra_headers() {
        let mut headers = HashMap::new();
        headers.insert("X-Custom".into(), "value".into());
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            extra_headers: headers,
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(req.headers().get("X-Custom").unwrap(), "value");
    }

    /// 回归测试：FunASR server (funasr_wss_server.py:841) 只接受 `binary` subprotocol，
    /// 握手不注入这个 header 会拿到 400 Bad Request。
    #[test]
    fn handshake_request_sets_binary_subprotocol() {
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        let sub = req
            .headers()
            .get("Sec-WebSocket-Protocol")
            .expect("Sec-WebSocket-Protocol 必须存在（FunASR server 要求 binary）");
        assert_eq!(sub, "binary");
    }

    /// 用户的 extra_headers 同名 header 可以覆盖默认 binary —— 便于对接自定义 server。
    #[test]
    fn handshake_request_user_subprotocol_overrides_default() {
        let mut headers = HashMap::new();
        headers.insert("Sec-WebSocket-Protocol".into(), "custom-proto".into());
        let client = FunasrClient::new(FunasrConfig {
            endpoint: "ws://127.0.0.1:10095/".into(),
            extra_headers: headers,
            ..Default::default()
        });
        let req = client.build_handshake_request().unwrap();
        assert_eq!(req.headers().get("Sec-WebSocket-Protocol").unwrap(), "custom-proto");
    }

    // ===== FunasrClose 构造 =====

    #[test]
    fn funasr_close_normal_is_1000_empty_reason() {
        // 正常关闭：code=1000、reason 空 —— 对齐浏览器 WS normal closure
        let c = FunasrClose::normal();
        assert_eq!(c.code, 1000);
        assert_eq!(c.reason, "");
    }

    #[test]
    fn funasr_close_abnormal_is_1006() {
        // 异常关闭：code=1006、reason 非空 —— next_event 内部用
        let c = FunasrClose::abnormal();
        assert_eq!(c.code, 1006);
        assert!(!c.reason.is_empty());
    }

    #[test]
    fn funasr_close_from_frame_carries_code_and_reason() {
        // 从 tungstenite CloseFrame 构造：code / reason 都应透传
        use async_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        let frame = CloseFrame {
            code: CloseCode::Away,
            reason: "server shutting down".into(),
        };
        let c = FunasrClose::from_frame(frame);
        assert_eq!(c.code, 1001);
        assert_eq!(c.reason, "server shutting down");
    }

    // ===== FunasrEvent 形态 =====

    #[test]
    fn funasr_event_message_carries_response() {
        // Message 变体必须能装 FunasrResponse 且 Debug 可打印
        let resp = FunasrResponse {
            mode: FunasrResponseMode::Online,
            text: "hi".into(),
            is_final: false,
        };
        let evt = FunasrEvent::Message(resp);
        // Debug 必须能格式化（避免后续日志 panic）
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Message(r) => {
                assert_eq!(r.text, "hi");
                assert!(!r.is_final);
            }
            _ => panic!("Message 变体应能 match 出 FunasrResponse"),
        }
    }

    #[test]
    fn funasr_event_close_carries_code() {
        // Close 变体装 FunasrClose —— 验证 code 透传
        let evt = FunasrEvent::Close(FunasrClose {
            code: 1000,
            reason: "ok".into(),
        });
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Close(c) => {
                assert_eq!(c.code, 1000);
                assert_eq!(c.reason, "ok");
            }
            _ => panic!("Close 变体应能 match 出 FunasrClose"),
        }
    }

    #[test]
    fn funasr_event_error_carries_client_error() {
        // Error 变体装 ClientError —— live_asr_api 上层要把它 Display 化下行
        let evt = FunasrEvent::Error(crate::client::error::ClientError::Ws("test".into()));
        let _ = format!("{:?}", evt);
        match evt {
            FunasrEvent::Error(e) => {
                assert!(matches!(e, crate::client::error::ClientError::Ws(_)));
            }
            _ => panic!("Error 变体应能 match 出 ClientError"),
        }
    }
}