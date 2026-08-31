//! vLLM-Omni TTS WebSocket 客户端。
//!
//! 客户端实例复用一条 WebSocket 长连接。首次合成时建立连接并发送
//! `session.config`，每个 utterance 发送 `input.text` 与 `input.done`，
//! 收到 `session.done` 后继续等待下一段文本。控制消息使用 JSON 文本帧，
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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::tts::{BoxStream, TtsClient};
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
}

impl TtsWsConfig {
    /// 构造 `session.config` 控制消息。
    fn session_config(&self, voice: Option<&str>) -> SessionConfig {
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

pub struct TtsWsClient {
    cfg: TtsWsConfig,
    connection: Arc<Mutex<Option<WsStream>>>,
}
impl TtsWsClient {
    /// 创建客户端。连接延迟到第一次 `synthesize`，避免启动时占用上游连接。
    pub fn new(cfg: TtsWsConfig) -> Self {
        Self {
            cfg,
            connection: Arc::new(Mutex::new(None)),
        }
    }
    async fn connect(&self, voice: Option<&str>) -> Result<WsStream, ClientError> {
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
        let config = serde_json::to_string(&self.cfg.session_config(voice))
            .map_err(|e| ClientError::Decode(format!("serialize session.config: {e}")))?;
        ws.send(Message::Text(config.into()))
            .await
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

    pub async fn close(&self) -> Result<(), ClientError> {
        let mut guard = self.connection.lock().await;
        if let Some(mut ws) = guard.take() {
            info!(target: "voice_server.tts.ws", endpoint = %self.cfg.endpoint, "TTS WebSocket 开始关闭");
            // 显式发送 session.close，让服务端立即释放会话资源。
            let close = serde_json::to_string(&control_message("session.close")).unwrap();
            ws.send(Message::Text(close.into()))
                .await
                .map_err(|e| {
                    warn!(target: "voice_server.tts.ws", endpoint = %self.cfg.endpoint, error = %e, "TTS WebSocket 发送 session.close 失败");
                    ClientError::Ws(format!("send session.close: {e}"))
                })?;
            ws.close(None)
                .await
                .map_err(|e| {
                    warn!(target: "voice_server.tts.ws", endpoint = %self.cfg.endpoint, error = %e, "TTS WebSocket 关闭失败");
                    ClientError::Ws(format!("close TTS WebSocket: {e}"))
                })?;
            info!(target: "voice_server.tts.ws", endpoint = %self.cfg.endpoint, "TTS WebSocket 已关闭");
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
    async fn synthesize(
        &self,
        session_id: &str,
        text: &str,
        _sample_rate_override: Option<u32>,
        voice_override: Option<String>,
    ) -> Result<BoxStream<Result<TtsEvent, ClientError>>, ClientError> {
        // 同一条连接上的 utterance 串行处理，直到收到 session.done 才释放锁。
        // input.done 只触发 flush，不会关闭连接，因此后续调用可以复用握手。
        let connection = Arc::clone(&self.connection);
        let client = Self {
            cfg: self.cfg.clone(),
            connection: Arc::clone(&connection),
        };
        let session_id = session_id.to_string();
        let text = text.to_string();
        let stream = async_stream::stream! {
            let mut guard = connection.lock().await;
            let reused = guard.is_some();
            info!(
                target: "voice_server.tts.ws",
                session_id,
                endpoint = %client.cfg.endpoint,
                text_chars = text.chars().count(),
                reused,
                "TTS WebSocket 开始合成"
            );
            if reused {
                debug!(target: "voice_server.tts.ws", session_id, "TTS WebSocket 复用已有连接");
            }
            if guard.is_none() {
                *guard = Some(match client.connect(voice_override.as_deref()).await {
                    Ok(ws) => ws,
                    Err(e) => {
                        warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 建连失败");
                        yield Err(e);
                        return;
                    }
                });
            }
            let ws = guard.as_mut().unwrap();
            if reused {
                if let Some(voice) = voice_override.as_deref() {
                    let config = serde_json::to_string(&client.cfg.session_config(Some(voice))).unwrap();
                    debug!(target: "voice_server.tts.ws", session_id, voice, "TTS WebSocket 更新 session.config");
                    if let Err(e) = ws.send(Message::Text(config.into())).await {
                        *guard = None;
                        warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 发送 session.config 失败");
                        yield Err(ClientError::Ws(format!("send session.config: {e}")));
                        return;
                    }
                }
            }
            let text_msg = serde_json::to_string(&input_text(&text)).unwrap();
            info!(target: "voice_server.tts.ws", session_id, text_chars = text.chars().count(), "TTS WebSocket 发送 input.text");
            if let Err(e) = ws.send(Message::Text(text_msg.into())).await {
                *guard = None;
                warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 发送 input.text 失败");
                yield Err(ClientError::Ws(format!("send input.text: {e}")));
                return;
            }
            let done_msg = serde_json::to_string(&control_message("input.done")).unwrap();
            if let Err(e) = ws.send(Message::Text(done_msg.into())).await {
                *guard = None;
                warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 发送 input.done 失败");
                yield Err(ClientError::Ws(format!("send input.done: {e}")));
                return;
            }
            debug!(target: "voice_server.tts.ws", session_id, "TTS WebSocket 已发送 input.done");
            let mut seq = 0;
            while let Some(message) = ws.next().await {
                match message {
                    Ok(Message::Binary(bytes)) => {
                        seq += 1;
                        debug!(target: "voice_server.tts.ws", session_id, seq, bytes = bytes.len(), "TTS WebSocket 收到音频帧");
                        yield Ok(TtsEvent { seq, data: bytes.to_vec(), is_last: false });
                    }
                    Ok(Message::Text(value)) => match parse_control_message(&value) {
                        Ok(ServerMessage::AudioStart) => debug!(target: "voice_server.tts.ws", session_id, "TTS WebSocket 收到 audio.start"),
                        Ok(ServerMessage::AudioDone { error: false }) => debug!(target: "voice_server.tts.ws", session_id, seq, "TTS WebSocket 收到 audio.done"),
                        Ok(ServerMessage::AudioDone { error: true }) => {
                            *guard = None;
                            warn!(target: "voice_server.tts.ws", session_id, seq, "TTS WebSocket 收到 audio.done(error=true)");
                            yield Err(ClientError::Ws("TTS audio generation failed".into()));
                            break;
                        }
                        Ok(ServerMessage::SessionDone) => {
                            info!(target: "voice_server.tts.ws", session_id, audio_frames = seq, "TTS WebSocket 收到 session.done");
                            yield Ok(TtsEvent { seq: seq + 1, data: Vec::new(), is_last: true });
                            break;
                        }
                        Ok(ServerMessage::Error(message)) => {
                            *guard = None;
                            warn!(target: "voice_server.tts.ws", session_id, error = %message, "TTS WebSocket 收到服务端错误");
                            yield Err(ClientError::Ws(message));
                            break;
                        }
                        Err(e) => {
                            warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 控制消息解析失败");
                            yield Err(e);
                            break;
                        }
                    },
                    Ok(Message::Ping(payload)) => {
                        debug!(target: "voice_server.tts.ws", session_id, bytes = payload.len(), "TTS WebSocket 收到 Ping");
                        let _ = ws.send(Message::Pong(payload)).await;
                    }
                    Ok(Message::Close(frame)) => {
                        *guard = None;
                        warn!(target: "voice_server.tts.ws", session_id, ?frame, "TTS WebSocket 被服务端关闭");
                        yield Err(ClientError::Ws("TTS WebSocket closed".into()));
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        *guard = None;
                        warn!(target: "voice_server.tts.ws", session_id, error = %e, "TTS WebSocket 接收失败");
                        yield Err(ClientError::Ws(format!("receive TTS WebSocket frame: {e}")));
                        break;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn session_config_contains_vllm_fields() {
        let cfg = TtsWsConfig {
            voice: Some("vivian".into()),
            language: Some("English".into()),
            response_format: "pcm".into(),
            stream_audio: true,
            ..Default::default()
        };
        let value = serde_json::to_value(cfg.session_config(None)).unwrap();
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
}
