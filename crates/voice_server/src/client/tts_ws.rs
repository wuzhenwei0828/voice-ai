//! vLLM-Omni TTS WebSocket 客户端。
//!
//! 客户端通过全局有界连接池复用多条 WebSocket 长连接。首次使用连接时发送
//! `session.config`，每个 utterance 发送 `input.text` 与 `input.done`，
//! 收到 `session.done` 后连接才可被下一 owner 使用。控制消息使用 JSON 文本帧，
//! 音频使用二进制帧。

use async_trait::async_trait;
use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::header::{HeaderName, HeaderValue},
    Message,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::tts::{BoxStream, TtsClient, TtsInputSession};
use crate::client::error::ClientError;
use crate::events::TtsEvent;

pub type WsStream = async_tungstenite::WebSocketStream<
    async_tungstenite::tokio::ClientStream<tokio::net::TcpStream>,
>;

#[derive(Debug, Clone)]
pub struct TtsWsConfig {
    /// WebSocket 地址，例如 `ws://127.0.0.1:8091/v1/audio/speech/stream`。
    pub endpoint: String,
    /// 可选 API Key，客户端会自动补充 `Bearer ` 前缀。
    pub api_key: Option<String>,
    /// 默认音色，名称由具体模型决定，例如 `vivian`。
    pub voice: Option<String>,
    /// Qwen3-TTS 任务类型：`CustomVoice`、`VoiceDesign` 或 `Base`。
    pub task_type: Option<String>,
    /// 合成语言；未指定时由模型自动判断。
    pub language: Option<String>,
    /// 风格、情绪等自然语言指令。
    pub instructions: Option<String>,
    /// 音频格式，WebSocket 场景通常使用 `pcm` 或 `wav`。
    pub response_format: String,
    /// 输出采样率（Hz）。None = provider 默认。
    pub sample_rate: Option<u32>,
    /// 输出声道数。
    pub channels: u8,
    /// 播放速度。
    pub speed: Option<f32>,
    /// Base 任务的参考音频。
    pub ref_audio: Option<String>,
    /// 参考音频文字转写。
    pub ref_text: Option<String>,
    /// 最大生成 token 数。
    pub max_new_tokens: Option<u32>,
    /// 是否将一句话拆成多个二进制 PCM 帧返回。
    pub stream_audio: bool,
    /// WebSocket 握手额外请求头。
    pub extra_headers: Vec<(String, String)>,
    /// 建连超时时间。
    pub timeout: Duration,
    /// 连接池最大并发连接数。
    pub max_connections: usize,
}

impl Default for TtsWsConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            api_key: None,
            voice: None,
            task_type: None,
            language: None,
            instructions: None,
            response_format: "pcm".into(),
            sample_rate: None,
            channels: 1,
            speed: None,
            ref_audio: None,
            ref_text: None,
            max_new_tokens: None,
            stream_audio: false,
            extra_headers: Vec::new(),
            timeout: Duration::from_secs(300),
            max_connections: 4,
        }
    }
}

#[derive(Debug, Serialize)]
struct SessionConfig {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    speed: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_audio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ref_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_new_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_rate: Option<u32>,
}

impl TtsWsConfig {
    fn session_config_with(
        &self,
        voice: Option<&str>,
        sample_rate_override: Option<u32>,
    ) -> SessionConfig {
        SessionConfig {
            kind: "session.config",
            voice: voice.map(str::to_string).or_else(|| self.voice.clone()),
            task_type: self.task_type.clone(),
            language: self.language.clone(),
            instructions: self.instructions.clone(),
            response_format: Some(self.response_format.clone()),
            speed: self.speed,
            ref_audio: self.ref_audio.clone(),
            ref_text: self.ref_text.clone(),
            max_new_tokens: self.max_new_tokens,
            stream_audio: self.stream_audio.then_some(true),
            sample_rate: sample_rate_override.or(self.sample_rate),
        }
    }
}

#[derive(Debug, Serialize)]
struct ControlMessage<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
}
#[derive(Debug, Serialize)]
struct InputText<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
}
fn input_text(text: &str) -> InputText<'_> {
    InputText {
        kind: "input.text",
        text,
    }
}
fn control_message(kind: &str) -> ControlMessage<'_> {
    ControlMessage { kind }
}

#[derive(Debug)]
/// 服务端控制帧的内部表示；音频通过二进制帧单独传输。
enum ServerMessage {
    AudioStart,
    Audio(Vec<u8>),
    AudioDone { error: bool },
    SessionDone,
    Error(String),
}
#[derive(Debug, Deserialize)]
struct ControlEnvelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    error: bool,
    #[serde(default)]
    message: Option<String>,
}
fn parse_control_message(value: &str) -> Result<ServerMessage, ClientError> {
    let msg: ControlEnvelope = serde_json::from_str(value)
        .map_err(|e| ClientError::Decode(format!("invalid TTS WebSocket message: {e}")))?;
    match msg.kind.as_str() {
        "audio.start" => Ok(ServerMessage::AudioStart),
        "audio.done" => Ok(ServerMessage::AudioDone { error: msg.error }),
        "session.done" => Ok(ServerMessage::SessionDone),
        "error" => Ok(ServerMessage::Error(
            msg.message.unwrap_or_else(|| "TTS WebSocket error".into()),
        )),
        _ => Err(ClientError::Decode(format!(
            "unknown TTS WebSocket message type: {}",
            msg.kind
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Connecting,
    Idle,
    InUse,
    Closed,
}

struct ConnectionMeta {
    state: ConnectionState,
    generation: u64,
    lease_id: u64,
    owner_session_id: Option<String>,
    last_sent_at: Instant,
    idle_since: Option<Instant>,
}

struct TtsWsEntry {
    stream: Mutex<Option<WsStream>>,
    meta: Mutex<ConnectionMeta>,
}

struct TtsWsPool {
    entries: Mutex<Vec<Arc<TtsWsEntry>>>,
    notify: Notify,
    max_connections: usize,
}

pub struct TtsWsClient {
    cfg: TtsWsConfig,
    pool: Arc<TtsWsPool>,
}

struct TtsWsLease {
    client: Arc<TtsWsClient>,
    entry: Arc<TtsWsEntry>,
    session_id: String,
    generation: u64,
    lease_id: u64,
    released: Arc<AtomicBool>,
}

pub struct TtsWsInputSession {
    client: Arc<TtsWsClient>,
    session_id: String,
    sample_rate_override: Option<u32>,
    voice_override: Option<String>,
    lease: Option<TtsWsLease>,
    config_sent: bool,
    utterance_text: String,
    utterance_state: UtteranceState,
    utterance_started_at: Option<Instant>,
    closed: bool,
    seq: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UtteranceState {
    Collecting,
    WaitingForDone,
}

impl TtsWsClient {
    /// 创建客户端。连接延迟到第一次 `open_input_session`/`synthesize`。
    pub fn new(cfg: TtsWsConfig) -> Self {
        let max_connections = cfg.max_connections.max(1);
        Self {
            cfg,
            pool: Arc::new(TtsWsPool {
                entries: Mutex::new(Vec::new()),
                notify: Notify::new(),
                max_connections,
            }),
        }
    }
    async fn connect(
        &self,
        voice: Option<&str>,
        sample_rate_override: Option<u32>,
    ) -> Result<WsStream, ClientError> {
        info!(
            target: "voice_server.tts.ws",
            endpoint = %self.cfg.endpoint,
            api_key_present = self.cfg.api_key.is_some(),
            extra_headers_count = self.cfg.extra_headers.len(),
            timeout_ms = self.cfg.timeout.as_millis() as u64,
            voice = voice.or(self.cfg.voice.as_deref()).unwrap_or(""),
            "TTS WebSocket 开始连接"
        );
        let mut request = self
            .cfg
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|e| ClientError::Ws(format!("invalid TTS WebSocket URL: {e}")))?;
        if let Some(key) = &self.cfg.api_key {
            let value = if key.starts_with("Bearer ") || key.starts_with("bearer ") {
                key.clone()
            } else {
                format!("Bearer {key}")
            };
            request.headers_mut().insert(
                "Authorization",
                value
                    .parse()
                    .map_err(|e| ClientError::Ws(format!("invalid authorization: {e}")))?,
            );
        }
        for (name, value) in &self.cfg.extra_headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| ClientError::Ws(format!("invalid header name: {e}")))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| ClientError::Ws(format!("invalid header value: {e}")))?;
            request.headers_mut().insert(name, value);
        }
        // 握手成功后，第一条业务消息必须是 session.config。
        let (mut ws, _) = tokio::time::timeout(self.cfg.timeout, connect_async(request))
            .await
            .map_err(|_| {
                warn!(
                    target: "voice_server.tts.ws",
                    endpoint = %self.cfg.endpoint,
                    timeout_ms = self.cfg.timeout.as_millis() as u64,
                    "TTS WebSocket 连接超时"
                );
                ClientError::Ws("TTS WebSocket connect timeout".into())
            })?
            .map_err(|e| {
                warn!(
                    target: "voice_server.tts.ws",
                    endpoint = %self.cfg.endpoint,
                    error = %e,
                    "TTS WebSocket 握手失败"
                );
                ClientError::Ws(format!("TTS WebSocket handshake: {e}"))
            })?;
        info!(
            target: "voice_server.tts.ws",
            endpoint = %self.cfg.endpoint,
            "TTS WebSocket 握手成功"
        );
        let config = self.config_payload(voice, sample_rate_override)?;
        tokio::time::timeout(
            Duration::from_secs(9),
            ws.send(Message::Text(config.into())),
        )
        .await
        .map_err(|_| ClientError::Ws("send session.config timeout".into()))?
        .map_err(|e| {
            warn!(
                target: "voice_server.tts.ws",
                endpoint = %self.cfg.endpoint,
                error = %e,
                "TTS WebSocket 发送 session.config 失败"
            );
            ClientError::Ws(format!("send session.config: {e}"))
        })?;
        debug!(target: "voice_server.tts.ws", "TTS WebSocket 已发送 session.config");
        Ok(ws)
    }

    fn config_payload(
        &self,
        voice: Option<&str>,
        sample_rate_override: Option<u32>,
    ) -> Result<String, ClientError> {
        serde_json::to_string(&self.cfg.session_config_with(voice, sample_rate_override))
            .map_err(|e| ClientError::Decode(format!("serialize session.config: {e}")))
    }

    async fn acquire(
        self: &Arc<Self>,
        session_id: &str,
        voice: Option<&str>,
        sample_rate_override: Option<u32>,
        send_config: bool,
    ) -> Result<TtsWsLease, ClientError> {
        loop {
            let config = self.config_payload(voice, sample_rate_override)?;
            // Arm the notification before scanning the pool so a release racing with
            // the scan cannot be lost between the capacity check and the await.
            let notified = self.pool.notify.notified();
            // Snapshot entries before awaiting individual metadata locks. The pool lock is
            // never held across metadata or network I/O.
            let entries_snapshot = self.pool.entries.lock().await.clone();
            let mut selected = None;
            for entry in entries_snapshot {
                let mut meta = entry.meta.lock().await;
                if meta.state == ConnectionState::Idle {
                    meta.state = ConnectionState::InUse;
                    meta.owner_session_id = Some(session_id.to_string());
                    meta.lease_id = meta.lease_id.wrapping_add(1).max(1);
                    meta.idle_since = None;
                    let generation = meta.generation;
                    let lease_id = meta.lease_id;
                    drop(meta);
                    selected = Some((entry, false, generation, lease_id));
                    break;
                }
            }
            let (entry, needs_connect, generation, lease_id) = if let Some(selected) = selected {
                selected
            } else {
                let mut entries = self.pool.entries.lock().await;
                if entries.len() < self.pool.max_connections {
                    let entry = Arc::new(TtsWsEntry {
                        stream: Mutex::new(None),
                        meta: Mutex::new(ConnectionMeta {
                            state: ConnectionState::Connecting,
                            generation: 1,
                            lease_id: 1,
                            owner_session_id: Some(session_id.to_string()),
                            last_sent_at: Instant::now(),
                            idle_since: None,
                        }),
                    });
                    let generation = 1;
                    let lease_id = 1;
                    entries.push(Arc::clone(&entry));
                    (entry, true, generation, lease_id)
                } else {
                    drop(entries);
                    tokio::time::timeout(self.cfg.timeout, notified)
                        .await
                        .map_err(|_| ClientError::Ws("TTS WebSocket pool wait timed out".into()))?;
                    continue;
                }
            };

            if needs_connect {
                info!(target: "voice_server.tts.ws", session_id, pool_size = self.pool.max_connections, "TTS WebSocket 预留连接并开始握手");
                match self.connect(voice, sample_rate_override).await {
                    Ok(ws) => {
                        *entry.stream.lock().await = Some(ws);
                        let mut meta = entry.meta.lock().await;
                        meta.state = ConnectionState::InUse;
                        meta.last_sent_at = Instant::now();
                    }
                    Err(error) => {
                        {
                            let mut meta = entry.meta.lock().await;
                            meta.state = ConnectionState::Closed;
                            meta.owner_session_id = None;
                            meta.generation = meta.generation.wrapping_add(1).max(1);
                        }
                        let mut entries = self.pool.entries.lock().await;
                        entries.retain(|candidate| !Arc::ptr_eq(candidate, &entry));
                        self.pool.notify.notify_waiters();
                        return Err(error);
                    }
                }
            } else {
                if send_config {
                    let mut stream = entry.stream.lock().await;
                    let result = stream
                        .as_mut()
                        .ok_or_else(|| ClientError::Ws("TTS WebSocket connection missing".into()))?
                        .send(Message::Text(config.clone().into()));
                    let result = match tokio::time::timeout(self.cfg.timeout, result).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => {
                            Err(ClientError::Ws(format!("send session.config: {error}")))
                        }
                        Err(_) => Err(ClientError::Ws("send session.config timeout".into())),
                    };
                    if let Err(error) = result {
                        drop(stream);
                        self.invalidate(&entry).await;
                        return Err(error);
                    }
                    drop(stream);
                    let mut meta = entry.meta.lock().await;
                    meta.last_sent_at = Instant::now();
                    debug!(target: "voice_server.tts.ws", session_id, "TTS WebSocket 新对话已发送 session.config");
                }
            }
            return Ok(TtsWsLease {
                client: Arc::clone(self),
                entry,
                session_id: session_id.to_string(),
                generation,
                lease_id,
                released: Arc::new(AtomicBool::new(false)),
            });
        }
    }

    async fn invalidate(&self, entry: &Arc<TtsWsEntry>) {
        {
            let mut meta = entry.meta.lock().await;
            meta.state = ConnectionState::Closed;
            meta.owner_session_id = None;
            meta.generation = meta.generation.wrapping_add(1).max(1);
            meta.idle_since = None;
        }
        if let Some(mut ws) = entry.stream.lock().await.take() {
            let close_message = serde_json::to_string(&control_message("session.close"))
                .unwrap_or_else(|_| r#"{"type":"session.close"}"#.to_string());
            let _ = tokio::time::timeout(
                Duration::from_secs(1),
                ws.send(Message::Text(close_message.into())),
            )
            .await;
            let _ = ws.close(None).await;
        }
        let mut entries = self.pool.entries.lock().await;
        entries.retain(|candidate| !Arc::ptr_eq(candidate, entry));
        self.pool.notify.notify_waiters();
    }

    fn schedule_reap(self: &Arc<Self>, entry: Arc<TtsWsEntry>, generation: u64, lease_id: u64) {
        let client = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            let should_reap = {
                let mut meta = entry.meta.lock().await;
                let should_reap = meta.state == ConnectionState::Idle
                    && meta.generation == generation
                    && meta.lease_id == lease_id
                    && meta
                        .idle_since
                        .map(|t| t.elapsed() >= Duration::from_secs(20))
                        .unwrap_or(false);
                if should_reap {
                    meta.state = ConnectionState::Closed;
                    meta.owner_session_id = None;
                    meta.idle_since = None;
                    meta.generation = meta.generation.wrapping_add(1).max(1);
                }
                should_reap
            };
            if should_reap {
                info!(target: "voice_server.tts.ws", "TTS WebSocket 空闲超过 20 秒，回收连接");
                // 上游不支持 Ping 保活；回收前显式发送 session.close，随后移除连接。
                if let Some(mut ws) = entry.stream.lock().await.take() {
                    let close = serde_json::to_string(&control_message("session.close")).unwrap();
                    let _ = ws.send(Message::Text(close.into())).await;
                    let _ = ws.close(None).await;
                }
                let mut entries = client.pool.entries.lock().await;
                entries.retain(|candidate| !Arc::ptr_eq(candidate, &entry));
                client.pool.notify.notify_waiters();
            }
        });
    }

    pub async fn close(&self) -> Result<(), ClientError> {
        let entries = self.pool.entries.lock().await.clone();
        for entry in entries {
            self.invalidate(&entry).await;
        }
        Ok(())
    }
}

#[async_trait]
impl TtsClient for TtsWsClient {
    fn output_format(&self) -> (Option<u32>, u8) {
        (self.cfg.sample_rate, self.cfg.channels)
    }

    fn default_voice_short(&self) -> &str {
        self.cfg.voice.as_deref().unwrap_or("vivian")
    }

    async fn open_input_session(
        &self,
        session_id: &str,
        sample_rate_override: Option<u32>,
        voice_override: Option<String>,
    ) -> Result<Option<Box<dyn super::tts::TtsInputSession>>, ClientError> {
        Ok(Some(Box::new(TtsWsInputSession {
            client: Arc::new(self.clone_for_session()),
            session_id: session_id.to_string(),
            sample_rate_override,
            voice_override,
            lease: None,
            config_sent: false,
            utterance_text: String::new(),
            utterance_state: UtteranceState::Collecting,
            utterance_started_at: None,
            closed: false,
            seq: 0,
        })))
    }

    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
        sample_rate_override: Option<u32>,
        voice_override: Option<String>,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        let mut input = TtsWsInputSession {
            client: Arc::new(self.clone_for_session()),
            session_id: session_id.to_string(),
            sample_rate_override,
            voice_override,
            lease: None,
            config_sent: false,
            utterance_text: String::new(),
            utterance_state: UtteranceState::Collecting,
            utterance_started_at: None,
            closed: false,
            seq: 0,
        };
        let session_id = session_id.to_string();
        let text = text.to_string();
        let stream = async_stream::stream! {
            info!(
                target: "voice_server.tts.ws",
                session_id,
                endpoint = %input.client.cfg.endpoint,
                text_chars = text.chars().count(),
                "TTS WebSocket 开始合成"
            );
            if let Err(e) = input.send_text(&text).await {
                yield Err(e);
                return;
            }
            if let Err(e) = input.flush().await {
                yield Err(e);
                return;
            }
            while let Some(event) = input.next_event().await {
                let terminal = matches!(&event, Ok(event) if event.is_last);
                yield event;
                if terminal {
                    if let Err(error) = input.finish().await {
                        yield Err(error);
                    }
                    break;
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

impl TtsWsClient {
    fn clone_for_session(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            pool: Arc::clone(&self.pool),
        }
    }
}

impl TtsWsLease {
    async fn check_owner(&self) -> Result<(), ClientError> {
        let meta = self.entry.meta.lock().await;
        if meta.state != ConnectionState::InUse
            || meta.generation != self.generation
            || meta.lease_id != self.lease_id
            || meta.owner_session_id.as_deref() != Some(self.session_id.as_str())
        {
            return Err(ClientError::Ws(
                "TTS WebSocket lease is no longer owned".into(),
            ));
        }
        Ok(())
    }

    async fn send(&self, message: Message, kind: &str) -> Result<(), ClientError> {
        self.check_owner().await?;
        let mut stream = self.entry.stream.lock().await;
        let ws = stream
            .as_mut()
            .ok_or_else(|| ClientError::Ws("TTS WebSocket connection missing".into()))?;
        tokio::time::timeout(self.client.cfg.timeout, ws.send(message))
            .await
            .map_err(|_| ClientError::Ws(format!("send {kind} timeout")))?
            .map_err(|error| ClientError::Ws(format!("send {kind}: {error}")))?;
        drop(stream);
        self.entry.meta.lock().await.last_sent_at = Instant::now();
        Ok(())
    }

    async fn next(&self) -> Option<Result<ServerMessage, ClientError>> {
        if let Err(error) = self.check_owner().await {
            return Some(Err(error));
        }
        let mut stream = self.entry.stream.lock().await;
        let ws = match stream.as_mut() {
            Some(ws) => ws,
            None => {
                return Some(Err(ClientError::Ws(
                    "TTS WebSocket connection missing".into(),
                )))
            }
        };
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(bytes))) => {
                    return Some(Ok(ServerMessage::Audio(bytes.to_vec())))
                }
                Some(Ok(Message::Text(value))) => return Some(parse_control_message(&value)),
                Some(Ok(Message::Ping(payload))) => {
                    let _ = ws.send(Message::Pong(payload)).await;
                }
                Some(Ok(Message::Close(_))) => {
                    return Some(Err(ClientError::Ws("TTS WebSocket closed".into())))
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    return Some(Err(ClientError::Ws(format!(
                        "receive TTS WebSocket frame: {error}"
                    ))))
                }
                None => return Some(Err(ClientError::Ws("TTS WebSocket stream ended".into()))),
            }
        }
    }

    async fn release(&self) {
        self.released.store(true, Ordering::Release);
        let mut meta = self.entry.meta.lock().await;
        if meta.state == ConnectionState::InUse
            && meta.generation == self.generation
            && meta.lease_id == self.lease_id
        {
            meta.state = ConnectionState::Idle;
            meta.owner_session_id = None;
            meta.idle_since = Some(Instant::now());
            self.client
                .schedule_reap(Arc::clone(&self.entry), self.generation, self.lease_id);
            self.client.pool.notify.notify_one();
        }
    }
}

impl Drop for TtsWsLease {
    fn drop(&mut self) {
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let client = Arc::clone(&self.client);
        let entry = Arc::clone(&self.entry);
        // A dropped compatibility stream represents cancellation. Invalidate in a
        // detached task because Drop cannot await the socket close.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                client.invalidate(&entry).await;
            });
        }
    }
}

#[async_trait]
impl super::tts::TtsInputSession for TtsWsInputSession {
    async fn send_text(&mut self, text: &str) -> Result<(), ClientError> {
        if self.closed {
            return Err(ClientError::Ws("TTS input session closed".into()));
        }
        if self.utterance_state == UtteranceState::WaitingForDone {
            return Err(ClientError::Ws(
                "cannot send text before previous utterance session.done".into(),
            ));
        }
        if self.lease.is_none() {
            let lease = self
                .client
                .acquire(
                    &self.session_id,
                    self.voice_override.as_deref(),
                    self.sample_rate_override,
                    !self.config_sent,
                )
                .await?;
            self.lease = Some(lease);
            self.config_sent = true;
        }
        let msg = serde_json::to_string(&input_text(text))
            .map_err(|e| ClientError::Decode(e.to_string()))?;
        let result = self
            .lease
            .as_ref()
            .unwrap()
            .send(Message::Text(msg.clone().into()), "input.text")
            .await;
        if result.is_err() {
            if let Some(lease) = self.lease.take() {
                self.client.invalidate(&lease.entry).await;
            }
            self.config_sent = false;
            let lease = self
                .client
                .acquire(
                    &self.session_id,
                    self.voice_override.as_deref(),
                    self.sample_rate_override,
                    true,
                )
                .await?;
            self.lease = Some(lease);
            self.config_sent = true;
            self.lease
                .as_ref()
                .unwrap()
                .send(Message::Text(msg.into()), "input.text")
                .await?;
        }
        self.utterance_text.push_str(text);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Err(ClientError::Ws("TTS input session closed".into()));
        }
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| ClientError::Ws("flush called before send_text".into()))?;
        let msg = serde_json::to_string(&control_message("input.done")).unwrap();
        let result = lease
            .send(Message::Text(msg.clone().into()), "input.done")
            .await;
        if result.is_err() {
            let pending = self.utterance_text.clone();
            if let Some(lease) = self.lease.take() {
                self.client.invalidate(&lease.entry).await;
            }
            self.config_sent = false;
            let lease = self
                .client
                .acquire(
                    &self.session_id,
                    self.voice_override.as_deref(),
                    self.sample_rate_override,
                    true,
                )
                .await?;
            self.lease = Some(lease);
            self.config_sent = true;
            if !pending.is_empty() {
                let text_msg = serde_json::to_string(&input_text(&pending))
                    .map_err(|e| ClientError::Decode(e.to_string()))?;
                self.lease
                    .as_ref()
                    .unwrap()
                    .send(Message::Text(text_msg.into()), "input.text")
                    .await?;
            }
            self.lease
                .as_ref()
                .unwrap()
                .send(Message::Text(msg.into()), "input.done")
                .await?;
        }
        self.utterance_started_at = Some(Instant::now());
        self.utterance_state = UtteranceState::WaitingForDone;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<Result<TtsEvent, ClientError>> {
        loop {
            let lease = self.lease.as_ref()?;
            let entry = Arc::clone(&lease.entry);
            let remaining = self
                .utterance_started_at
                .map(|started| self.client.cfg.timeout.saturating_sub(started.elapsed()))
                .unwrap_or(self.client.cfg.timeout);
            if remaining.is_zero() {
                self.client.invalidate(&entry).await;
                self.lease = None;
                self.config_sent = false;
                self.utterance_started_at = None;
                return Some(Err(ClientError::Ws(
                    "TTS WebSocket request timed out".into(),
                )));
            }
            let next = match tokio::time::timeout(remaining, lease.next()).await {
                Ok(next) => next?,
                Err(_) => {
                    self.client.invalidate(&entry).await;
                    self.lease = None;
                    self.config_sent = false;
                    self.utterance_started_at = None;
                    return Some(Err(ClientError::Ws(
                        "TTS WebSocket request timed out".into(),
                    )));
                }
            };
            match next {
                Ok(ServerMessage::Audio(bytes)) => {
                    self.seq = self.seq.saturating_add(1);
                    return Some(Ok(TtsEvent {
                        seq: self.seq,
                        data: bytes,
                        is_last: false,
                    }));
                }
                Ok(ServerMessage::AudioStart | ServerMessage::AudioDone { error: false }) => {
                    continue;
                }
                Ok(ServerMessage::AudioDone { error: true }) => {
                    self.client.invalidate(&entry).await;
                    self.lease = None;
                    self.config_sent = false;
                    self.utterance_started_at = None;
                    return Some(Err(ClientError::Ws("TTS audio generation failed".into())));
                }
                Ok(ServerMessage::SessionDone) => {
                    self.utterance_text.clear();
                    self.utterance_state = UtteranceState::Collecting;
                    self.utterance_started_at = None;
                    self.seq = self.seq.saturating_add(1);
                    return Some(Ok(TtsEvent {
                        seq: self.seq,
                        data: Vec::new(),
                        is_last: true,
                    }));
                }
                Ok(ServerMessage::Error(_message)) => {
                    self.client.invalidate(&entry).await;
                    self.lease = None;
                    self.config_sent = false;
                    self.utterance_started_at = None;
                    return Some(Err(ClientError::Ws("TTS WebSocket provider error".into())));
                }
                Err(error) => {
                    self.client.invalidate(&entry).await;
                    self.lease = None;
                    self.config_sent = false;
                    self.utterance_started_at = None;
                    return Some(Err(error));
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), ClientError> {
        self.closed = true;
        if let Some(lease) = self.lease.take() {
            let close = serde_json::to_string(&control_message("session.close")).unwrap();
            let _ = lease
                .send(Message::Text(close.into()), "session.close")
                .await;
            self.client.invalidate(&lease.entry).await;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), ClientError> {
        self.closed = true;
        if let Some(lease) = self.lease.take() {
            lease.release().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_tungstenite::tokio::accept_async;
    use futures_util::StreamExt;
    use tokio::net::TcpListener;

    fn test_entry(state: ConnectionState, generation: u64, lease_id: u64) -> Arc<TtsWsEntry> {
        Arc::new(TtsWsEntry {
            stream: Mutex::new(None),
            meta: Mutex::new(ConnectionMeta {
                state,
                generation,
                lease_id,
                owner_session_id: (state == ConnectionState::InUse).then(|| "owner".into()),
                last_sent_at: Instant::now(),
                idle_since: (state == ConnectionState::Idle).then_some(Instant::now()),
            }),
        })
    }
    #[test]
    fn session_config_contains_vllm_fields() {
        let cfg = TtsWsConfig {
            voice: Some("vivian".into()),
            language: Some("English".into()),
            response_format: "pcm".into(),
            stream_audio: true,
            ..Default::default()
        };
        let value = serde_json::to_value(cfg.session_config_with(None, None)).unwrap();
        assert_eq!(value["type"], "session.config");
        assert_eq!(value["voice"], "vivian");
        assert_eq!(value["stream_audio"], true);
    }
    #[test]
    fn builds_input_control_messages() {
        assert_eq!(
            serde_json::to_string(&input_text("hello")).unwrap(),
            r#"{"type":"input.text","text":"hello"}"#
        );
        assert_eq!(
            serde_json::to_string(&control_message("input.done")).unwrap(),
            r#"{"type":"input.done"}"#
        );
    }
    #[test]
    fn parses_server_control_messages() {
        assert!(matches!(
            parse_control_message(r#"{"type":"audio.start"}"#).unwrap(),
            ServerMessage::AudioStart
        ));
        assert!(matches!(
            parse_control_message(r#"{"type":"session.done"}"#).unwrap(),
            ServerMessage::SessionDone
        ));
    }

    #[test]
    fn websocket_pool_clamps_zero_capacity() {
        let client = TtsWsClient::new(TtsWsConfig {
            max_connections: 0,
            ..Default::default()
        });
        assert_eq!(client.pool.max_connections, 1);
    }

    #[tokio::test]
    async fn pool_assigns_distinct_idle_entries_to_concurrent_owners() {
        let client = Arc::new(TtsWsClient::new(TtsWsConfig {
            max_connections: 2,
            ..Default::default()
        }));
        let first = test_entry(ConnectionState::Idle, 1, 1);
        let second = test_entry(ConnectionState::Idle, 1, 1);
        client
            .pool
            .entries
            .lock()
            .await
            .extend([Arc::clone(&first), Arc::clone(&second)]);

        let first_lease = client
            .acquire("session-a", None, None, false)
            .await
            .unwrap();
        let second_lease = client
            .acquire("session-b", None, None, false)
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first_lease.entry, &second_lease.entry));
        assert_eq!(first.meta.lock().await.state, ConnectionState::InUse);
        assert_eq!(second.meta.lock().await.state, ConnectionState::InUse);
        first_lease.release().await;
        second_lease.release().await;
    }

    #[tokio::test]
    async fn pool_waiter_observes_a_racing_release_notification() {
        let client = Arc::new(TtsWsClient::new(TtsWsConfig {
            max_connections: 1,
            timeout: Duration::from_secs(1),
            ..Default::default()
        }));
        let entry = test_entry(ConnectionState::InUse, 1, 1);
        client.pool.entries.lock().await.push(Arc::clone(&entry));
        let waiter_client = Arc::clone(&client);
        let waiter = tokio::spawn(async move {
            waiter_client
                .acquire("waiting-session", None, None, false)
                .await
        });

        tokio::task::yield_now().await;
        {
            let mut meta = entry.meta.lock().await;
            meta.state = ConnectionState::Idle;
            meta.owner_session_id = None;
            meta.idle_since = Some(Instant::now());
        }
        client.pool.notify.notify_one();

        let lease = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("pool waiter should wake after release")
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&lease.entry, &entry));
        lease.release().await;
    }

    #[tokio::test(start_paused = true)]
    async fn idle_reaper_removes_connection_after_twenty_seconds() {
        let client = Arc::new(TtsWsClient::new(TtsWsConfig::default()));
        let entry = test_entry(ConnectionState::Idle, 1, 1);
        client.pool.entries.lock().await.push(Arc::clone(&entry));
        client.schedule_reap(Arc::clone(&entry), 1, 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(40)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(client.pool.entries.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn stale_idle_reaper_cannot_remove_a_reassigned_connection() {
        let client = Arc::new(TtsWsClient::new(TtsWsConfig::default()));
        let entry = test_entry(ConnectionState::Idle, 1, 1);
        client.pool.entries.lock().await.push(Arc::clone(&entry));
        client.schedule_reap(Arc::clone(&entry), 1, 1);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        {
            let mut meta = entry.meta.lock().await;
            meta.state = ConnectionState::InUse;
            meta.generation = 2;
            meta.lease_id = 2;
            meta.owner_session_id = Some("new-owner".into());
            meta.idle_since = None;
        }
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::task::yield_now().await;

        assert_eq!(client.pool.entries.lock().await.len(), 1);
        assert_eq!(entry.meta.lock().await.state, ConnectionState::InUse);
        assert_eq!(
            entry.meta.lock().await.owner_session_id.as_deref(),
            Some("new-owner")
        );
    }

    #[tokio::test]
    async fn websocket_protocol_keeps_session_config_first_and_reuses_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let mut messages = Vec::new();
            let mut flush_count = 0;
            while let Some(Ok(message)) = ws.next().await {
                match message {
                    Message::Text(text) => {
                        let text = text.to_string();
                        messages.push(text.clone());
                        if text == r#"{"type":"input.done"}"# {
                            flush_count += 1;
                            ws.send(Message::Text(r#"{"type":"audio.start"}"#.into()))
                                .await
                                .unwrap();
                            ws.send(Message::Binary(vec![1, 0, 2, 0].into()))
                                .await
                                .unwrap();
                            ws.send(Message::Text(r#"{"type":"audio.done"}"#.into()))
                                .await
                                .unwrap();
                            ws.send(Message::Text(r#"{"type":"session.done"}"#.into()))
                                .await
                                .unwrap();
                            if flush_count == 2 {
                                break;
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            messages
        });

        let client = TtsWsClient::new(TtsWsConfig {
            endpoint: format!("ws://{address}/v1/audio/speech/stream"),
            timeout: Duration::from_secs(2),
            voice: Some("vivian".into()),
            ..Default::default()
        });
        let mut session = client
            .open_input_session("protocol-session", None, None)
            .await
            .unwrap()
            .unwrap();

        session.send_text("第一句。").await.unwrap();
        session.flush().await.unwrap();
        while !session.next_event().await.unwrap().unwrap().is_last {}

        session.send_text("第二句。").await.unwrap();
        session.flush().await.unwrap();
        while !session.next_event().await.unwrap().unwrap().is_last {}
        session.finish().await.unwrap();

        let messages = server.await.unwrap();
        assert_eq!(
            messages[0],
            r#"{"type":"session.config","voice":"vivian","response_format":"pcm"}"#
        );
        assert_eq!(
            messages,
            vec![
                r#"{"type":"session.config","voice":"vivian","response_format":"pcm"}"#,
                r#"{"type":"input.text","text":"第一句。"}"#,
                r#"{"type":"input.done"}"#,
                r#"{"type":"input.text","text":"第二句。"}"#,
                r#"{"type":"input.done"}"#,
            ]
        );
    }
}
